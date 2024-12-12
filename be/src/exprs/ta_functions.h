// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "column/column_viewer.h"
#include "exprs/function_helper.h"

namespace starrocks {
class TaFunctions {
public:
    /**
     * @param:
     * @paramType columns: [TYPE_DOUBLE, TYPE_DOUBLE, TYPE_BIGINT, TYPE_DOUBLE, TYPE_DOUBLE]
     * @return TYPE_VARCHAR
     */
    DEFINE_VECTORIZED_FN(get_distribute_group_str);

    /**
     * @param:
     * @paramType columns: [TYPE_DATETIME, TYPE_BIGINT]
     * @return TYPE_INT
     */
    DEFINE_VECTORIZED_FN(funnel_pack_time);

    /**
     * @param:
     * @paramType columns: [TYPE_ARRAY_BIGINT, TYPE_BIGINT, TYPE_INT]
     * @return TYPE_BIGINT
     */
    DEFINE_VECTORIZED_FN(funnel_max_step);

    /**
     * @param:
     * @paramType columns: [TYPE_MAP(INT,ARRAY_DATE_TIME), TYPE_BIGINT, TYPE_DATETIME,TYPE_DATETIME]
     * @return TYPE_MAP(DATETIME,BIGINT)
     */
    DEFINE_VECTORIZED_FN(funnel_max_step_date);

    DEFINE_VECTORIZED_FN(is_retention_user_in_date_collect);

    static int64_t microsToMillis(int64_t micros) {
        return std::floor(micros / 1000);
    }

    static int64_t millisToMicros(int64_t millis) {
        return millis * 1000;
    }

    static int64_t millsToSecsCeil(int64_t millis) {
        return (int64_t) std::ceil(millis / 1000.0);
    }

    enum RangeType {
        LAST_DAYS,
        RECENT_DAYS,
        THIS_WEEK,
        THIS_MONTH,
        TIME_RANGE
    };
    using OptionalTimestamp = std::optional<TimestampValue>;
    using TimestampPair = std::pair<OptionalTimestamp, OptionalTimestamp>;
    using TimestampSet = std::set<TimestampValue>;
    /**
     *
     * @param:
     * @paramType columns: [TYPE_DATETIME, TYPE_VARCHAR, TYPE_VARCHAR] or
     *                     [TYPE_DATETIME, TYPE_VARCHAR, TYPE_VARCHAR, TYPE_DATETIME, TYPE_DATETIME] or
     *                     [TYPE_DATETIME, TYPE_VARCHAR, TYPE_VARCHAR, TYPE_DATETIME, TYPE_DATETIME, ARRAY_DATETIME]
     * @return ARRAY_DATETIME
     */
    DEFINE_VECTORIZED_FN(ta_extend_date);

    DEFINE_VECTORIZED_FN(get_kudu_array);

private:
    static const std::map<Slice, RangeType> sliceToRangeType;
    constexpr static std::string_view kudu_array_delimiter = "\t";

    static void get_distribute_group_str_inner(double minVal, double maxVal, int64_t discreteLimit, int64_t number,double statVal, Slice* slice);
    static void getGroupStr(double statVal, int64_t d, double minVal, double maxVal,Slice* slice);
    static void getNumLengthAndFirstVal(int64_t num,int* length, int* firstVal);
    static int64_t getGroupIndex(double val, int64_t  d);
    static void doubleToString(double value, Slice* slice);
    static void multiDoubleToString(double value1,double value2,int precision,Slice* slice);
    static void leftCommaDoubleToString(double value, starrocks::Slice* slice);
    static void rightCommaDoubleToString(const double value, starrocks::Slice* slice);
    static void getSimpleGroupStr(double statVal0, double minVal0, double maxVal0, Slice* pSlice);
    static Buffer<TimestampPair> getRangeIntersection(const Buffer<TimestampPair>& belongRange,
        const TimestampValue& startDate, const TimestampValue& endDate);
    static StatusOr<Buffer<TimestampPair>> getBelongRange(const ColumnPtr& timestamps, const RangeType& rangeType, const Slice& rangeParam);
    static Buffer<TimestampValue> getExtraBlock(const ColumnPtr& extraBlock);
    static Buffer<Buffer<size_t>> getExtraTimestamps(const Buffer<TimestampValue>& extraBlock, const Buffer<TimestampPair>& belongRange,
        const TimestampValue& startTimestamp, const TimestampValue& endTimestamp);
};

} // namespace starrocks
