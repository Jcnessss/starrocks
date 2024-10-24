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

#include "exprs/ta_functions.h"

#include <iomanip>

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "gutil/map_util.h"

namespace starrocks {

StatusOr<ColumnPtr> TaFunctions::get_distribute_group_str(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto minVal_viewer = ColumnViewer<TYPE_DOUBLE> (columns[0]);
    auto maxVal_viewer = ColumnViewer<TYPE_DOUBLE> (columns[1]);
    auto discreteLimit_viewer = ColumnViewer<TYPE_BIGINT>(columns[2]);
    auto number_viewer = ColumnViewer<TYPE_BIGINT> (columns[3]);
    auto startVal_viewer = ColumnViewer<TYPE_DOUBLE> (columns[4]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> builder(size);
    for (size_t i = 0; i < size; ++i) {
        if (startVal_viewer.is_null(i)) {
            builder.append_null();
            continue;
        }
        char buf[129];
        Slice temp_slice(buf,129);
        const auto& minVal = minVal_viewer.value(i);
        const auto& maxVal = maxVal_viewer.value(i);
        const auto& discreteLimit = discreteLimit_viewer.value(i);
        const auto& number = number_viewer.value(i);
        const auto& statVal = startVal_viewer.value(i);

        get_distribute_group_str_inner(minVal, maxVal, discreteLimit, number, statVal, &temp_slice);
        builder.append(temp_slice);
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

void TaFunctions::get_distribute_group_str_inner(double minVal, double maxVal, int64_t discreteLimit, int64_t number,double statVal, Slice* slice) {
    if (number <= discreteLimit) {
        doubleToString(statVal, slice);
        return;
    }
    double s = maxVal - minVal; //2000
    if (s <= 10) {
       getSimpleGroupStr(statVal, minVal, maxVal, slice);
       return;
    }

    int64_t dMax = (s / 6);    // 333
    int64_t dMin = (s / 10);   // 200

    int dMaxLength; // 3
    int dMaxFirstVal; // 3
    int dMinLength; //3
    int dMinFirstVal; // 2

    getNumLengthAndFirstVal(dMax,&dMaxLength,&dMaxFirstVal);
    getNumLengthAndFirstVal(dMin,&dMinLength,&dMinFirstVal);

    int64_t d;
    if (dMaxLength != dMinLength) {
        d = exp10_int64(dMaxLength-1);
    } else if (dMaxFirstVal != dMinFirstVal) {
        d = dMaxFirstVal* exp10_int64(dMaxLength-1);
    } else {
        d = (dMaxFirstVal+0.5)* exp10_int64(dMaxLength-1);
    }
    getGroupStr(statVal, d, minVal, maxVal,slice);
    return;
}

void TaFunctions::getNumLengthAndFirstVal(int64_t num, int* length, int* firstVal) {
    // TODO: using head zero count to get the first digit
    int count = 1;
    int firstDigitNum = num;
    while ((num=num / 10) != 0) {
        count++;
        firstDigitNum = num;
    }
    *length = count;
    *firstVal = firstDigitNum;
}

//2000 300 0 2000
void TaFunctions::getGroupStr(double statVal, int64_t d, double minVal, double maxVal, Slice* slice) {
    int64_t minIndex = getGroupIndex(minVal, d); // 0
    int64_t maxIndex = getGroupIndex(maxVal, d); // 6
    int64_t index = getGroupIndex(statVal, d); // 12
    if (index <= minIndex) {
        auto number =  fmt::format_int((index+1) * d);
        slice->mutable_data()[0] = ',';
        memcpy(slice->mutable_data()+1, number.data(), number.size());
        slice->truncate(number.size() + 1);
        return;
    } else if (index >= maxIndex) {
        auto number =  fmt::format_int(index * d);
        memcpy(slice->mutable_data(), number.data(), number.size());
        slice->mutable_data()[number.size()] = ',';
        slice->truncate(number.size() + 1);
        return;
    }
    auto number1 = fmt::format_int(index * d);
    memcpy(slice->mutable_data(), number1.data(), number1.size());
    slice->mutable_data()[number1.size()] = ',';
    auto number2 = fmt::format_int((index + 1) * d);
    memcpy(slice->mutable_data()+number1.size()+1, number2.data(), number2.size());
    slice->truncate(number1.size()+number2.size()+1);
    return;
}

int64_t TaFunctions::getGroupIndex(double val, int64_t d) {
    if (val < 0) {
        if (std::fmod(val, d) == 0) {
            return int64_t(val / d);
        } else {
            return int64_t(val / d - 1);
        }
    }
    return int64_t(val / d);
}

void TaFunctions::doubleToString(double value, Slice* slice) {
    DecimalV2Value decimal_value;
    decimal_value.assign_from_double(value);
    int pos = decimal_value.to_string(slice->mutable_data(), 2);
    slice->truncate(pos);
}

void TaFunctions::multiDoubleToString(double value1,double value2,int precision,Slice* slice) {
    DecimalV2Value decimal_value1;
    decimal_value1.assign_from_double(value1);
    int pos = decimal_value1.to_string(slice->mutable_data(),precision);
    slice->mutable_data()[pos] = ',';
    DecimalV2Value decimal_value2;
    decimal_value2.assign_from_double(value2);
    pos = decimal_value2.to_string(slice->mutable_data() + pos + 1,precision) + pos;
    slice->truncate(pos + 1);
}

void TaFunctions::getSimpleGroupStr(double statVal0, double minVal0, double maxVal0, Slice* slice) {
    double min = std::floor(minVal0);
    double max = std::ceil(maxVal0);
    double statVal = std::round(statVal0);

    int defParts = 10;
    double step = (max - min) / defParts;
    step = std::round(step);

    if (step <= 0) {
        multiDoubleToString(min, max, 1, slice);
        return;
    }
    double lowLimit = min;
    for (int i = 0; i < defParts; i++) {
        double upLimit = lowLimit + step;
        if (i == 0) {
            if (statVal < upLimit) {
                leftCommaDoubleToString(upLimit, slice);
                return;
            }
        } else if (i == defParts - 1) {
            if (lowLimit <= statVal) {
                rightCommaDoubleToString(lowLimit, slice);
                return;
            }
        } else {
            if (lowLimit <= statVal && statVal < upLimit) {
                multiDoubleToString(lowLimit, upLimit, 2, slice);
                return;
            }
        }
        lowLimit = upLimit;
    }
    multiDoubleToString(min, max, 1, slice);
    return ;
}

void TaFunctions::leftCommaDoubleToString(double value, starrocks::Slice* slice) {
    DecimalV2Value decimal_value;
    decimal_value.assign_from_double(value);
    slice->mutable_data()[0] = ',';
    int pos = decimal_value.to_string(slice->mutable_data() + 1, 2);
    slice->truncate(pos+1);
}

void TaFunctions::rightCommaDoubleToString(const double value, starrocks::Slice* slice) {
    DecimalV2Value decimal_value;
    decimal_value.assign_from_double(value);
    int pos = decimal_value.to_string(slice->mutable_data(), 2);
    slice->mutable_data()[pos] = ',';
    slice->truncate(pos+1);
}


StatusOr<ColumnPtr> TaFunctions::funnel_pack_time(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto datetime_viewer = ColumnViewer<TYPE_DATETIME>(columns[0]);
    auto funnel_index_viewer = ColumnViewer<TYPE_INT>(columns[1]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> builder(size);
    for (size_t i = 0; i < size; ++i) {
        if (datetime_viewer.is_null(i) || funnel_index_viewer.is_null(i)) {
            builder.append_null();
            continue;
        }
        auto funnel_index = funnel_index_viewer.value(i);
        if (funnel_index > FUNNEL_INDEX_MASK) {
            throw std::runtime_error("funnelIndex overflow: " + std::to_string(funnel_index));
        }
        auto datetime = datetime_viewer.value(i);
        int64_t unix_timestamp = datetime.to_unix_microsecond();
        int64_t mills = unix_timestamp / 1000;
        int64_t shiftedMills = mills << MILLIS_SHIFT;
        if (shiftedMills >> MILLIS_SHIFT != mills) {
            throw std::runtime_error("Millis overflow: " + std::to_string(shiftedMills));
        }
        builder.append(shiftedMills | (funnel_index & FUNNEL_INDEX_MASK));
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}
} // namespace starrocks
