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

private:
    static void get_distribute_group_str_inner(double minVal, double maxVal, int64_t discreteLimit, int64_t number,double statVal, Slice* slice);
    static void getGroupStr(double statVal, int64_t d, double minVal, double maxVal,Slice* slice);
    static void getNumLengthAndFirstVal(int64_t num,int* length, int* firstVal);
    static int64_t  getGroupIndex(double val, int64_t  d);
    static void doubleToString(double value, Slice* slice);
    static void multiDoubleToString(double value1,double value2,int precision,Slice* slice);
    static void leftCommaDoubleToString(double value, starrocks::Slice* slice);
    static void rightCommaDoubleToString(const double value, starrocks::Slice* slice);
    static void getSimpleGroupStr(double statVal0, double minVal0, double maxVal0, Slice* pSlice);
};

} // namespace starrocks
