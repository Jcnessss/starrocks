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

#include <codecvt>
#include <iomanip>
#include <utility>


#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/array_column.h"
#include "gutil/gscoped_ptr.h"
#include "column/map_column.h"
#include "gutil/map_util.h"
#include "gutil/strings/split.h"
#include "gutil/strings/stringpiece.h"
#include "util/pinyin.hpp"

#include "runtime/mem_pool.h"


namespace starrocks {

static inline uint64_t bool_values[] = {(1UL << 31) - 1, (1UL << 30) - 1, (1UL << 29) - 1, (1UL << 28) - 1, (1UL << 27) - 1,
                                        (1UL << 26) - 1, (1UL << 25) - 1, (1UL << 24) - 1, (1UL << 23) - 1, (1UL << 22) - 1,
                                        (1UL << 21) - 1, (1UL << 20) - 1, (1UL << 19) - 1, (1UL << 18) - 1, (1UL << 17) - 1,
                                        (1UL << 16) - 1, (1UL << 15) - 1, (1UL << 14) - 1, (1UL << 13) - 1, (1UL << 12) - 1,
                                        (1UL << 11) - 1, (1UL << 10) - 1, (1UL << 9) - 1, (1UL << 8) - 1, (1UL << 7) - 1,
                                        (1UL << 6) - 1, (1UL << 5) - 1, (1UL << 4) - 1, (1UL << 3) - 1, (1UL << 2) - 1, (1UL << 1) - 1};

struct TimestampArray {
    int64_t* elements;
    int32_t size;
    int64_t operator[](int i) const noexcept { return elements[i]; }
    void add(int64_t timestamp) {
        elements[size++] = timestamp;
    }
    bool empty() {
        return size == 0;
    }
    int64_t* begin() {
        return elements;
    }
    int64_t* end() {
        return elements + size;
    }
};

static constexpr int64_t DAY_GAP_MILLS = 86400000L;
static constexpr int32_t FUNNEL_INDEX_MASK = 0xFFF;
static constexpr int32_t MILLIS_SHIFT = 12;

class MaxStepFinder{
public:
    MaxStepFinder(TimestampArray* stepTimestamps,int maxRow, std::vector<int32_t> positions,int64_t windows_gap);
    int64_t find();
    static int64_t roundFloorToDay(int64_t timestamp);
private:
    TimestampArray* step_time_stamps;
    int32_t max_row;
    std::vector<int32_t> positions;
    std::vector<int64_t> limit_from_step0;
    int64_t limit_max;
    bool visit_any_step_max_valid_timestamp = false;
    int32_t current_step = -1;

    bool moveCurrentStepPositionAndAdjust();
    bool advance_step();
    void adjustPrevious(int32_t step);
};

MaxStepFinder::MaxStepFinder(TimestampArray* stepTimestamps,int32_t maxRow, std::vector<int32_t> positions,int64_t windows_gap) {
    step_time_stamps = stepTimestamps;
    max_row = maxRow;
    this->positions = std::move(positions);

    const TimestampArray step0_timestamps = stepTimestamps[0];
    const int32_t length = step0_timestamps.size;
    std::vector<int64_t> limit(length,0);
    for (int i = 0; i < length; i++) {
        const int64_t start_time = step0_timestamps[i];
        if (windows_gap>0){
            limit[i] = start_time+windows_gap;
        } else if (windows_gap==-1){
            const int64_t start_time_trunc_day= roundFloorToDay(start_time);
            limit[i] = start_time_trunc_day+DAY_GAP_MILLS;
        }else{
            throw std::runtime_error(fmt::format("invalid windows gap {} is not a valid number",windows_gap));
        }
    }
    limit_from_step0 = limit;
    limit_max = limit[length-1];
}

int64_t MaxStepFinder::find() {
    do{
        while (advance_step()){
            adjustPrevious(current_step);
        }
        if(current_step==max_row){
            break;
        }
    } while (moveCurrentStepPositionAndAdjust());
    return current_step;
}

bool MaxStepFinder::moveCurrentStepPositionAndAdjust(){
    while (!visit_any_step_max_valid_timestamp){
        TimestampArray timestamps = step_time_stamps[current_step];
        const int32_t p = positions[current_step];
        if (p>=timestamps.size){
            throw std::runtime_error(fmt::format("already visited the max timestamp of step {}" , current_step));
        }
        positions[current_step]= p +1;
        const int64_t current_time_stamp = step_time_stamps[current_step][positions[current_step]];
        if(p+1 == timestamps.size-1||current_time_stamp>=limit_max){
            visit_any_step_max_valid_timestamp = true;
            if (current_time_stamp>limit_max){
                break;
            }
        }
        adjustPrevious(current_step);
        if (const int64_t limit0=limit_from_step0[positions[0]]; current_time_stamp<limit0){
            return true;
        }
    }
    return false;
}

bool MaxStepFinder::advance_step(){
    int32_t step = current_step+1;
    if (step > max_row){
        return false;
    }
    TimestampArray timestamp = step_time_stamps[step];
    const int32_t p = positions[step];
    if (p >= timestamp.size){
        throw std::runtime_error(fmt::format("already visited the max timestamp of step {} " , step));
    }
    int32_t np = 0;
    if (step > 0){
        const int64_t current_time_stamp = step_time_stamps[current_step][positions[current_step]];
        int32_t maxLe = p;
        while(maxLe+1<timestamp.size && timestamp[maxLe+1]<=current_time_stamp){
            maxLe++;
        }
        positions[step]=maxLe;
        if (maxLe == timestamp.size-1){
            visit_any_step_max_valid_timestamp = true;
            np = maxLe;
        }else{
            int64_t limit0 = limit_from_step0[positions[0]];
            int64_t timestamp0 = timestamp[maxLe+1];
            if (timestamp0 < limit0){
                np = maxLe+1;
            }else{
                np=maxLe;
                if (timestamp0>= limit_max){
                    visit_any_step_max_valid_timestamp = true;
                }
            }
        }
    }else{
        np = p +1;
    }
    if (np>positions[step]){
        current_step=step;
        positions[step]=np;
        if (np ==timestamp.size-1){
            visit_any_step_max_valid_timestamp= true;
        }
        return true;
    } else {
        return false;
    }
}

void MaxStepFinder::adjustPrevious(int32_t step){
    while(step>0){
        int64_t time = step_time_stamps[step][positions[step]];
        int32_t pStep = step-1;
        TimestampArray& pStepTimestamp = step_time_stamps[pStep];
        int32_t pStepIndex = positions[pStep];
        while (pStepIndex+1<pStepTimestamp.size
                &&pStepTimestamp[pStepIndex+1]<time){
            pStepIndex++;
        }
        if (pStepIndex>positions[pStep]){
            positions[pStep]=pStepIndex;
            if (pStepIndex==pStepTimestamp.size-1){
                visit_any_step_max_valid_timestamp=true;
            }
            step=pStep;
        }else{
            break;
        }
    }
}
int64_t MaxStepFinder::roundFloorToDay(int64_t milliseconds) {
    return cctz::convert(
        cctz::civil_day(
            cctz::convert(
                std::chrono::system_clock::from_time_t(milliseconds / 1000),
                cctz::utc_time_zone()
            )
        ),
        cctz::utc_time_zone()
    ).time_since_epoch().count() * 1000;
}

static void toMatrix(const ColumnViewer<TYPE_BIGINT>& array,
                     size_t start,
                     size_t end,
                     int64_t* timestamps,
                     TimestampArray* stepTimestamps,
                     int64_t max_funnel_index) {
    size_t size[max_funnel_index];
    memset(size, 0, sizeof(size_t)*max_funnel_index);
    for (size_t j = start; j < end; j++) {
        const int64_t packed_time_millis = array.value(j);
        const int32_t funnel_index = static_cast<int32_t>(packed_time_millis & FUNNEL_INDEX_MASK);
        if (funnel_index < 1 || funnel_index > max_funnel_index) {
            continue;
        }
        const int32_t f = funnel_index - 1;
        size[f]++;
    }
    for (int i = 0; i < max_funnel_index; i++) {
        stepTimestamps[i].elements = timestamps;
        stepTimestamps[i].size = 0;
        timestamps+=size[i];
    }
    for (size_t j = start; j < end; j++) {
        const int64_t packed_time_millis = array.value(j);
        const int32_t funnel_index = static_cast<int32_t>(packed_time_millis & FUNNEL_INDEX_MASK);
        if (funnel_index < 1 || funnel_index > max_funnel_index) {
            continue;
        }
        const int32_t f = funnel_index - 1;
        stepTimestamps[f].add(static_cast<int64_t>(packed_time_millis) >> MILLIS_SHIFT);
    }

    for (size_t i = 0; i < max_funnel_index; i++) {
        if (!stepTimestamps[i].empty()){
            std::sort(stepTimestamps[i].begin(), stepTimestamps[i].end());
        }
    }
}

static int64_t funnel_max_step_inner(const ColumnViewer<TYPE_BIGINT>& array,
                              size_t start,
                              size_t end,
                              int64_t windows_gap,
                              int64_t max_funnel_index,
                              int64_t* timestamps,
                              TimestampArray* stepTimestamps) {
    if (end - start <= 0) {
        return 0;
    }
    toMatrix(array, start, end,timestamps,stepTimestamps, max_funnel_index);
    int maxKey = -1;
    while(maxKey + 1 < max_funnel_index && !stepTimestamps[maxKey + 1].empty()) {
        maxKey+=1;
    }
    if (maxKey<=0){
        return maxKey+1;
    }
    std::vector positions (maxKey+1,-1);
    return MaxStepFinder(stepTimestamps,maxKey,positions,windows_gap).find()+1;
}


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

void TaFunctions::leftCommaDoubleToString(double value, Slice* slice) {
    DecimalV2Value decimal_value;
    decimal_value.assign_from_double(value);
    slice->mutable_data()[0] = ',';
    int pos = decimal_value.to_string(slice->mutable_data() + 1, 2);
    slice->truncate(pos+1);
}

void TaFunctions::rightCommaDoubleToString(const double value, Slice* slice) {
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
        int64_t mills = std::floor(unix_timestamp / 1000);
        int64_t shiftedMills = mills << MILLIS_SHIFT;
        if (shiftedMills >> MILLIS_SHIFT != mills) {
            throw std::runtime_error("Millis overflow: " + std::to_string(shiftedMills));
        }
        builder.append(shiftedMills | (funnel_index & FUNNEL_INDEX_MASK));
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}


StatusOr<ColumnPtr> TaFunctions::funnel_max_step(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    auto* array = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]).get();
    auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(array));
    auto& array_offsets = col_array->offsets();

    auto viewer = ColumnViewer<TYPE_BIGINT>(col_array->elements_column());
    auto windows_viewer = ColumnHelper::get_const_value<TYPE_BIGINT>(columns[1]);
    auto maxFunnelIndex_viewer = ColumnHelper::get_const_value<TYPE_INT>(columns[2]);
    const size_t num_rows = array_offsets.size();

    ColumnBuilder<TYPE_BIGINT> builder(num_rows);
    // TODO: pre-allocation a large vector
    size_t max_elements = 0;
    for (size_t i = 0; i < num_rows-1; i++) {
        const size_t start = array_offsets.get_data()[i];
        const size_t end = array_offsets.get_data()[i+1];
        max_elements = std::max(max_elements, end - start);
    }
    // Global timestamp container allow the whole function to share the same memory pool

    gscoped_array timestamps(new int64_t[max_elements]);
    context->add_mem_usage(sizeof(int64_t)*max_elements);

    for (size_t i = 0; i < num_rows-1; i++) {
        const size_t start = array_offsets.get_data()[i];
        const size_t end = array_offsets.get_data()[i+1];

        if (maxFunnelIndex_viewer > 128) {
            throw std::runtime_error(fmt::format("funnelIndex overflow: {}", maxFunnelIndex_viewer));
        }
        // Each step has a timestamp array, and each timestamp array has a size
        TimestampArray stepTimestamps [maxFunnelIndex_viewer];
        memset(stepTimestamps, 0, sizeof(TimestampArray)*maxFunnelIndex_viewer);
        int64_t funnel_max_step = funnel_max_step_inner(
                viewer,
                start,
                end,
                windows_viewer,
                maxFunnelIndex_viewer,
                timestamps.get(),
                stepTimestamps);
        builder.append(funnel_max_step);
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

static phmap::flat_hash_map<int64_t, int64_t> funnel_max_step_date_inner(const ColumnViewer<TYPE_BIGINT>& array,
                                       size_t start,
                                       size_t end,
                                       int64_t windows_gap,
                                       int64_t max_funnel_index,
                                       TimestampValue start_time,
                                       TimestampValue end_time,
                                       int64_t* timestamps,
                                       TimestampArray* stepTimestamps);

StatusOr<ColumnPtr> TaFunctions::funnel_max_step_date([[maybe_unused]]starrocks::FunctionContext* context, const starrocks::Columns& columns){
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    auto* array = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]).get();
    auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(array));
    auto& array_offsets = col_array->offsets();

    const auto viewer = ColumnViewer<TYPE_BIGINT>(col_array->elements_column());
    const auto constant_num = context->get_num_constant_columns();

    const auto windows_gap_viewer =  ColumnHelper::get_const_value<TYPE_BIGINT>(columns[constant_num-4]);
    const auto funnel_index_viewer =  ColumnHelper::get_const_value<TYPE_INT>(columns[constant_num-3]);
    const auto start_time_viewer =  ColumnHelper::get_const_value<TYPE_DATETIME>(columns[constant_num-2]);
    const auto end_time_viewer = ColumnHelper::get_const_value<TYPE_DATETIME>(columns[constant_num-1]);

    const size_t num_rows = array_offsets.size();

    size_t max_elements = 0;
    size_t max_funnel_index = 0;
    for (size_t i = 0; i < num_rows-1; i++) {
        size_t start = array_offsets.get_data()[i];
        size_t end = array_offsets.get_data()[i+1];
        max_elements = std::max(max_elements, end - start);
    }

    gscoped_array timestamps(new int64_t[max_elements]);
    context->add_mem_usage(sizeof(int64_t)*max_elements);

    ColumnBuilder<TYPE_DATETIME> key_builder(num_rows);
    ColumnBuilder<TYPE_BIGINT> value_builder(num_rows);
    UInt32Column::Ptr new_offsets = UInt32Column::create();
    new_offsets->reserve(num_rows + 1);
    auto& offsets_vec = new_offsets->get_data();
    offsets_vec.push_back(0);

    for (size_t i = 0; i < num_rows - 1 ; ++i) {
        size_t start = array_offsets.get_data()[i];
        size_t end = array_offsets.get_data()[i+1];
        if (funnel_index_viewer > 128) {
            throw std::runtime_error(fmt::format("funnelIndex overflow: {}", max_funnel_index));
        }
        TimestampArray stepTimestamps [funnel_index_viewer];
        memset(stepTimestamps, 0, sizeof(TimestampArray)*funnel_index_viewer);
        phmap::flat_hash_map<int64_t, int64_t> result =funnel_max_step_date_inner(viewer,
                                   start,
                                   end,
                                   windows_gap_viewer,
                                   funnel_index_viewer,
                                   start_time_viewer,
                           end_time_viewer,
                                   timestamps.get(),
                                   stepTimestamps);
        for(auto& [key,value]:result){
            const int64_t unixSeconds = key / 1000;
            const int64_t unixMillis = (key % 1000)* 1000;
            TimestampValue timestampValue;
            timestampValue.from_unixtime(unixSeconds,unixMillis,cctz::utc_time_zone());
            key_builder.append(timestampValue);
            value_builder.append(value);
        }
        new_offsets->append(key_builder.data_column()->size());
    }
    return MapColumn::create(key_builder.build_nullable_column(), value_builder.build_nullable_column(),new_offsets);
}

int adjustStartPositions(TimestampArray* stepTimestamps, std::vector<int32_t>& positions, int maxKey);
static phmap::flat_hash_map<int64_t, int64_t> funnel_max_step_date_inner(const ColumnViewer<TYPE_BIGINT>& array,
                                       size_t start,
                                       size_t end,
                                       int64_t windows_gap,
                                       int64_t max_funnel_index,
                                       TimestampValue start_time,
                                       TimestampValue end_time,
                                       int64_t* timestamps,
                                       TimestampArray* stepTimestamps) {
    phmap::flat_hash_map<int64_t, int64_t> resultMap;
    {
        int64_t key = TaFunctions::microsToMillis(start_time.to_unix_microsecond());
        int64_t end_timestamp = TaFunctions::microsToMillis(end_time.to_unix_microsecond());
        while (key <= end_timestamp) {
            resultMap[key]=0L;
            key += DAY_GAP_MILLS;
        }
    }
    int entry_count = end-start;
    if (entry_count <= 0) {
        return resultMap;
    }
    toMatrix(array, start, end, timestamps, stepTimestamps, max_funnel_index);
    int maxKey = -1;
    while(maxKey+1<max_funnel_index && !stepTimestamps[maxKey + 1].empty()) {
        maxKey++;
    }
    if (maxKey<0){
        return resultMap;
    }

    std::vector<int32_t> positions(maxKey+1, -1);
    phmap::flat_hash_map<int64_t ,std::vector<int64_t>> step0map;
    for (int i = 0; i < stepTimestamps[0].size; i++) {
        int64_t timestamp = stepTimestamps[0][i];
        int64_t group = MaxStepFinder::roundFloorToDay(timestamp);
        if (step0map.contains(group)) {
            step0map[group].emplace_back(timestamp);
        }else {
            step0map[group] = std::vector{timestamp};
        }
    }
    for (auto& [key,value] : step0map) {
        int64_t maxStep;
        if (maxKey<=0) {
            maxStep=1;
        }else{
            stepTimestamps[0] = TimestampArray{value.data(),static_cast<int32_t>(value.size())};
            maxKey = adjustStartPositions(stepTimestamps, positions, maxKey);
            const std::vector copy(positions.begin(), positions.begin()+maxKey+1);
            maxStep = MaxStepFinder(stepTimestamps, maxKey,copy, windows_gap).find() + 1;
        }
        resultMap[key]=maxStep;
    }
    return resultMap;

}

int adjustStartPositions(TimestampArray* stepTimestamps, std::vector<int32_t>& positions, int maxKey) {
    int64_t start_time = stepTimestamps[0][0];
    for (int step = 1; step <= maxKey; step++) {
        auto timestamps = stepTimestamps[step];
        int p = positions[step];
        while (p+1 < timestamps.size && timestamps[p+1] <= start_time) {
            p++;
        }
        if (p==timestamps.size-1){
            return step-1;
        }
        positions[step]=p;
    }
    return maxKey;
}

int week(DateValue v) {
    auto julian_day = v.julian();

    return julian_day  / 7 + 1;
}

int month(DateValue v) {
    int y, m, d;
    v.to_date(&y, &m, &d);
    return m;
}

int year(DateValue v) {
    int y, m, d;
    v.to_date(&y, &m, &d);
    return y;
}

int get_unit_diff(DateValue value, DateValue init_date, const std::string _time_unit) {
    int diff = 0;
    if (_time_unit == "day") {
        diff = value.julian() - init_date.julian();
    } else if (_time_unit == "week") {
        diff = week(value) - week(init_date);
    } else if (_time_unit == "month") {
        int init_year = year(init_date);
        int value_year = year(value);
        diff = month(value) - month(init_date) + (value_year - init_year) * 12;
    }
    return std::abs(diff);
}

StatusOr<ColumnPtr> TaFunctions::is_retention_user_in_date_collect(FunctionContext* context, const Columns& columns) {
    const int64_t split = 0xFFFFFFFF;

    auto init_viewer = ColumnViewer<TYPE_VARBINARY>(columns[0]);
    auto return_viewer = ColumnViewer<TYPE_VARBINARY>(columns[1]);

    ColumnPtr column = context->get_constant_column(2);
    TimestampValue timestamp = ColumnHelper::get_const_value<TYPE_DATETIME>(column);
    DateValue init_date = DateValue{timestamp::to_julian(timestamp._timestamp)};
    ColumnPtr column2 = context->get_constant_column(3);
    auto unit_num = ColumnHelper::get_const_value<TYPE_INT>(column2);
    ColumnPtr column3 = context->get_constant_column(4);
    auto unit = ColumnHelper::get_const_value<TYPE_VARCHAR>(column3).to_string();
    ColumnPtr column4 = context->get_constant_column(5);
    auto type = ColumnHelper::get_const_value<TYPE_INT>(column4);

    int max_month_diff = 0;
    if (unit == "day") {
        max_month_diff = unit_num / 28 + 1;
    } else if (unit == "week") {
        max_month_diff = unit_num / 4 + 1;
    } else if (unit == "month") {
        max_month_diff = unit_num;
    }

    int init_year, init_month, init_day;
    init_date.to_date(&init_year, &init_month, &init_day);
    int32_t init_key = init_year * 100 + init_month;

    ColumnBuilder<TYPE_BOOLEAN> res(columns[0]->size());

    for (int row_num = 0; row_num < columns[0]->size(); row_num++) {
        Slice init = init_viewer.value(row_num);
        Slice return_date = return_viewer.value(row_num);
        std::map<int, std::vector<DateValue>> init_dates;
        if (init.size == 0 || return_date.size == 0) {
            continue ;
        }
        size_t offset = 0;
        size_t size;
        std::memcpy(&size, init.data + offset, sizeof(size_t));
        if (size == 0) {
            continue ;
        }
        offset += sizeof(size_t);
        bool is_found = false;
        for (int i = 0; i < size; i++) {
            uint64_t date;
            std::memcpy(&date, init.data + offset, sizeof(uint64_t));
            offset += sizeof(uint64_t);
            int year_month = date >> 32;
            if (year_month > init_key) {
                is_found = false;
                break;
            }
            if (year_month < init_key) {
                continue ;
            }
            uint32_t bitmap = date & split;
            while (bitmap > 0) {
                int day = __builtin_clz(bitmap) + 1;
                DateValue v;
                v.from_date(year_month / 100, year_month % 100, day);
                if (day == init_day) {
                    is_found = true;
                    break;
                }
                bitmap &= bool_values[day - 1];
            }
            if (is_found) {
                break;
            }
        }
        if (!is_found) {
            res.append(false);
            continue ;
        }

        offset = 0;
        std::memcpy(&size, return_date.data + offset, sizeof(size_t));
        offset += sizeof(size_t);
        is_found = false;
        for (int i = 0; i < size; i++) {
            uint64_t date;
            std::memcpy(&date, return_date.data + offset, sizeof(uint64_t));
            offset += sizeof(uint64_t);
            int year_month = date >> 32;
            int year = year_month / 100;
            int month = year_month % 100;
            uint32_t bitmap = date & split;
            if (year_month > init_key + max_month_diff) {
                is_found = false;
                break;
            }
            while (bitmap > 0) {
                int day = __builtin_clz(bitmap) + 1;
                DateValue v;
                v.from_date(year, month, day);
                int diff = get_unit_diff(v, init_date, unit);
                if (diff > unit_num - 1 && v > init_date) {
                    break;
                }
                if (type == 0) {
                    if (diff == unit_num - 1 && v >= init_date) {
                        is_found = true;
                        break;
                    }
                } else {
                    if (diff <= unit_num - 1 && v <= init_date) {
                        is_found = true;
                        break;
                    }
                }
                bitmap &= bool_values[day - 1];
            }
            if (is_found) {
                break;
            }
        }
        res.append(is_found & !type);
    }
    return res.build(ColumnHelper::is_all_const(columns));
}

const std::map<Slice, TaFunctions::RangeType> TaFunctions::sliceToRangeType = {
    {"last_days", TaFunctions::RangeType::LAST_DAYS},
    {"recent_days", TaFunctions::RangeType::RECENT_DAYS},
    {"this_week", TaFunctions::RangeType::THIS_WEEK},
    {"this_month", TaFunctions::RangeType::THIS_MONTH},
    {"time_range", TaFunctions::RangeType::TIME_RANGE}
};

StatusOr<ColumnPtr> TaFunctions::ta_extend_date(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});

    auto& timestamps = columns[0];
    auto& range_type = ColumnViewer<TYPE_VARCHAR> (columns[1]).value(0);
    auto& range_param = ColumnViewer<TYPE_VARCHAR> (columns[2]).value(0);

    const auto& s = getBelongRange(timestamps, sliceToRangeType.at(range_type), range_param);
    if (!s.ok()) {
        return to_status(s.status());
    }

    const auto& belong_range = s.value();

    const auto& extra_block = [&]() -> Buffer<TimestampValue> {
       if (columns.size() == 6) {
           const ColumnPtr& extra_block_column = columns[5];
           return getExtraBlock(extra_block_column);
       } else {
           return {};
       }
    }();

    const auto& [range_intersection, extra_timestamp] = [&]() {
        Buffer<Buffer<size_t>> dummy;
        if (columns.size() == 3) {
            return std::make_pair(belong_range, std::move(dummy));
        } else {
            const auto& start_timestamp = ColumnViewer<TYPE_DATETIME> ( columns[3]).value(0);
            const auto& end_timestamp = ColumnViewer<TYPE_DATETIME> ( columns[4]).value(0);
            const auto& intersection = getRangeIntersection(belong_range, start_timestamp, end_timestamp);

            if (columns.size() == 5) {
                return std::make_pair(std::move(intersection), std::move(dummy));
            }
            const auto& extra = getExtraTimestamps(extra_block, belong_range, start_timestamp, end_timestamp);
            return std::make_pair(std::move(intersection), extra);
        }
    }();

    size_t chunk_size = timestamps->size();

    UInt32Column::Ptr array_offsets = UInt32Column::create();
    array_offsets->reserve(chunk_size + 1);
    array_offsets->append(0);

    ColumnBuilder<TYPE_DATETIME> builder(chunk_size);

    const auto& data_range = [&]() {
        if (columns.size() == 3) {
            return belong_range;
        } else {
            return range_intersection;
        }
    }();

    size_t offset = 0;
    for (size_t row = 0; row < chunk_size; ++row) {
        const auto& range = data_range[row];
        bool has_valid_range = range.first.has_value() && range.second.has_value();

        // 如果本行 range 是 null
        if (!has_valid_range) {
            // 如果本行 extra_timestamp 为空，结果为 null 或 []
            if (extra_timestamp.empty() || extra_timestamp[row].empty()) {
                array_offsets->append(offset);
                continue;
            }

            // 追加 extra_timestamp
            for (const auto& index : extra_timestamp[row]) {
                const auto& t = extra_block[index];
                int64_t unix_time = t.to_unix_microsecond();
                int64_t second = unix_time / 1000000;
                int64_t micro = unix_time % 1000000;
                TimestampValue to_add;
                to_add.from_unixtime(second, micro, cctz::utc_time_zone());
                builder.append(std::move(to_add));
            }
            offset += extra_timestamp[row].size();
            array_offsets->append(offset);
            continue;
        }

        // 本行 range 有效，处理日期范围
        const TimestampValue& lower = range.first.value();
        const TimestampValue& upper = range.second.value();

        // 计算范围内的天数
        size_t days_in_range = ((DateValue)upper).julian() - ((DateValue)lower).julian() + 1;

        // 追加范围内的日期
        for (size_t idx = 0; idx < days_in_range; ++idx) {
            const auto& new_date = lower.add<DAY>(idx);
            int64_t unix_time = new_date.to_unix_second();
            TimestampValue new_timestamp;
            new_timestamp.from_unixtime(unix_time,0,cctz::utc_time_zone());
            builder.append(std::move(new_timestamp));
        }
        offset += days_in_range;

        // 追加 extra_timestamp 的元素到 builder
        if (!extra_timestamp.empty() && !extra_timestamp[row].empty()) {
            for (const auto& index : extra_timestamp[row]) {
                const auto& t = extra_block[index];
                int64_t unix_time = t.to_unix_microsecond();
                int64_t second = unix_time / 1000000;
                int64_t micro = unix_time % 1000000;
                TimestampValue to_add;
                to_add.from_unixtime(second, micro, cctz::utc_time_zone());
                builder.append(std::move(to_add));
            }
            offset += extra_timestamp[row].size();
        }

        array_offsets->append(offset);
    }
    if (timestamps->has_null()) {
        return NullableColumn::create(ArrayColumn::create(builder.build_nullable_column(), array_offsets),
                                      NullColumn::create(*ColumnHelper::as_raw_column<NullableColumn>(timestamps)->null_column()));
    } else {
        return ArrayColumn::create(builder.build_nullable_column(), array_offsets);
    }
}

StatusOr<Buffer<TaFunctions::TimestampPair>> TaFunctions::getBelongRange(const ColumnPtr& timestamps,
                                                                                const RangeType& rangeType,
                                                                                const Slice& rangeParam) {
    Buffer<TaFunctions::TimestampPair> range;
    size_t number = 0;
    if (timestamps->is_constant()) {
        number = 1;
    } else {
        number = timestamps->size();
    }
    const auto& timestamp_viewer = ColumnViewer<TYPE_DATETIME>(timestamps);
    for (size_t row = 0; row < number; row++) {
        if (timestamp_viewer.is_null(row)) {
            range.emplace_back();
        } else {
            const DateValue event_date = static_cast<DateValue>(timestamp_viewer.value(row));
            switch (rangeType) {
                case THIS_MONTH : {
                    DateValue start_date = event_date;
                    start_date.trunc_to_month();

                    DateValue end_date = event_date;
                    end_date.set_end_of_month();
                    range.emplace_back(std::move(start_date), std::move(end_date));
                } break;
                case THIS_WEEK: {
                    const int first_day_of_week = std::stoi(std::string(rangeParam.data, rangeParam.size));
                    if (first_day_of_week < 1 || first_day_of_week > 7) {
                        return Status::InvalidArgument("firstDayOfWeek should be in [1, 7]");
                    }
                    DateValue week_start_date = event_date;
                    week_start_date.trunc_to_week();
                    if (first_day_of_week == 1) {
                        range.emplace_back(std::move(week_start_date), week_start_date.add<DAY>(6));
                    } else {
                        DateValue fix_week_start_date = week_start_date.add<DAY>(first_day_of_week - 1);
                        if (fix_week_start_date > event_date) {
                            fix_week_start_date._julian -= 7;
                        }
                        range.emplace_back(std::move(fix_week_start_date), fix_week_start_date.add<DAY>(6));
                    }
                } break;
                case LAST_DAYS:
                case RECENT_DAYS:
                case TIME_RANGE: {
                    std::vector<std::string> params = strings::Split(StringPiece(rangeParam.data, rangeParam.size), "~");
                    const int range_lower = std::stoi(params[0]);
                    const int range_upper = std::stoi(params[1]);
                    if (range_lower > range_upper) {
                        return Status::InvalidArgument("The rangeLower must be less than or equal to the rangeUpper");
                    }
                    DateValue lower_date = event_date;
                    lower_date._julian -= range_upper;
                    DateValue upper_date = event_date;
                    upper_date._julian -= range_lower;
                    range.emplace_back(std::move(lower_date), std::move(upper_date));
                } break;
                default:
                    return Status::InvalidArgument(fmt::format("Invalid range type: {}", rangeType));
            }
        }
    }
    return range;
}

Buffer<TaFunctions::TimestampPair> TaFunctions::getRangeIntersection(const Buffer<TaFunctions::TimestampPair>& belongRange,
                                                                            const TimestampValue& startDate, const TimestampValue& endDate) {
    Buffer<TaFunctions::TimestampPair> range_intersection;
    for (size_t row = 0; row < belongRange.size(); row++) {
        const auto& lower = belongRange[row].first;
        const auto& upper = belongRange[row].second;
        if (!lower || !upper) {
            range_intersection.emplace_back();
        } else if (static_cast<DateValue>(startDate) > static_cast<DateValue>(upper.value())
        || static_cast<DateValue>(endDate) < static_cast<DateValue>(lower.value())) {
            range_intersection.emplace_back();
        } else {
            DateValue new_lower = static_cast<DateValue>(startDate) > static_cast<DateValue>(lower.value()) ? startDate : lower.value();
            DateValue new_upper = static_cast<DateValue>(endDate) < static_cast<DateValue>(upper.value()) ? endDate : upper.value();
            range_intersection.emplace_back(std::move(new_lower), std::move(new_upper));
        }

    }
    return range_intersection;
}

Buffer<TimestampValue> TaFunctions::getExtraBlock(const ColumnPtr& extraBlock) {
    if (extraBlock->only_null()) {
        return {};
    }
    ArrayColumn* array_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(extraBlock.get()));
    const auto& viewer = ColumnViewer<TYPE_DATETIME>(array_column->elements_column());
    std::set<TimestampValue> set;
    for (size_t i = 0; i < viewer.size(); i++) {
        const TimestampValue& timestamp = viewer.value(i);
        set.insert(timestamp);
    }
    return {set.begin(), set.end()};
}

Buffer<Buffer<size_t>> TaFunctions::getExtraTimestamps(
        const Buffer<TimestampValue>& extraBlock, const Buffer<TaFunctions::TimestampPair>& belongRange,
        const TimestampValue& startTimestamp, const TimestampValue& endTimestamp) {
    Buffer<Buffer<size_t>> extra;
    for (size_t row = 0; row < belongRange.size(); row++) {
        const auto& lower = belongRange[row].first;
        const auto& upper = belongRange[row].second;

        if (!lower || !upper) {
            extra.push_back({});
        } else {
            for (size_t index = 0; index < extraBlock.size(); index++) {
                const auto& timestamp = extraBlock[index];
                Buffer<size_t> curr_row;
                if ((DateValue)timestamp < (DateValue)startTimestamp || (DateValue)timestamp > (DateValue)endTimestamp) {
                    if (lower.has_value() && upper.has_value()
                        &&(DateValue)lower.value() <= (DateValue)timestamp
                        && (DateValue)upper.value() >= (DateValue)timestamp) {
                        curr_row.push_back(index);
                    }
                }
                extra.push_back(curr_row);
            }
        }
    }
    return extra;
}

StatusOr<ColumnPtr> TaFunctions::get_kudu_array(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto& string_column = columns[0];

    size_t chunk_size = string_column->size();

    ColumnBuilder<TYPE_VARCHAR> builder(chunk_size);

    UInt32Column::Ptr array_offsets = UInt32Column::create();
    array_offsets->reserve(chunk_size + 1);
    array_offsets->append(0);

    const auto& strings_viewer = ColumnViewer<TYPE_VARCHAR> (string_column);

    size_t offset = 0;
    for (size_t row = 0; row < chunk_size; row++) {
        if (strings_viewer.is_null(row)) {
            array_offsets->append(offset);
            continue;
        }

        const Slice& strings = strings_viewer.value(row);
        std::vector<StringPiece> curr_row = strings::Split(StringPiece(strings.data, strings.size), kudu_array_delimiter.data());
        for (const StringPiece& piece : curr_row) {
            builder.append(Slice(piece.data(), piece.size()));
            offset++;
        }
        array_offsets->append(offset);
    }
    if (string_column->has_null()) {
        return NullableColumn::create(ArrayColumn::create(builder.build_nullable_column(), array_offsets),
                                      NullColumn::create(*ColumnHelper::as_raw_column<NullableColumn>(string_column)->null_column()));
    } else {
        return ArrayColumn::create(builder.build_nullable_column(), array_offsets);
    }
}

StatusOr<ColumnPtr> TaFunctions::ta_convert_to_pinyin(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto& string_column = columns[0];

    size_t chunk_size = string_column->size();

    ColumnBuilder<TYPE_VARCHAR> builder(chunk_size);

    const auto& strings_viewer = ColumnViewer<TYPE_VARCHAR> (string_column);

    for (size_t row = 0; row < chunk_size; row++) {
        if (strings_viewer.is_null(row)) {
            builder.append_null();
            continue;
        }
        const Slice& str = strings_viewer.value(row);
        if (is_all_ascii(str)) {
            builder.append(str);
            continue;
        }
        const std::wstring& wstr = utf8_to_wstring(str);
        std::string curr_row;
        for (wchar_t wchar : wstr) {
            const auto& pinyin = WzhePinYin::Pinyin::GetPinyinOne(wchar);
            if (pinyin.has_value()) {
                curr_row.append(std::move(pinyin.value()));
            } else {
                if (wchar <= 0x7F) { // ASCII 范围内
                    curr_row.append(1, static_cast<char>(wchar));
                } else {
                    curr_row.append(std::move(wchar_to_utf8(wchar)));
                }
            }
        }
        builder.append(std::move(curr_row));
    }
    if (string_column->is_nullable()) {
        return builder.build_nullable_column();
    } else {
        return builder.build(string_column->is_constant());
    }
}


std::wstring TaFunctions::utf8_to_wstring(const Slice& slice) {
    thread_local std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
    return converter.from_bytes(slice.data, slice.data + slice.size);
}
std::string TaFunctions::wchar_to_utf8(const wchar_t& wchar) {
    thread_local std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
    return converter.to_bytes(std::move(std::wstring(1, wchar)));
}
bool TaFunctions::is_all_ascii(const Slice& slice) {
    const uint8_t* data = reinterpret_cast<const uint8_t*>(slice.data);
    const uint8_t* end = data + slice.size;

    while (data < end) {
        if (*data++ >= 0x80) return false;
    }
    return true;
}





} // namespace starrocks