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
#include <utility>


#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/array_column.h"
#include "gutil/gscoped_ptr.h"
#include "column/map_column.h"
#include "gutil/map_util.h"

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
    size_t size;
    int64_t operator[](int i) const noexcept { return elements[i]; }
    void add(int64_t timestamp) {
        elements[size++] = timestamp;
    }
    bool empty() {
        return size == 0;
    }
    int64_t* begin() {
        return this->elements;
    }
    int64_t* end() {
        return this->elements + this->size;
    }
};

static const int64_t DAY_GAP_MILLS = 86400000L;
static const int64_t FUNNEL_INDEX_MASK = 0xFFF;
static const int32_t MILLIS_SHIFT = 12;

class MaxStepFinder{
public:
    MaxStepFinder(TimestampArray* stepTimestamps,int maxRow,std::vector<int64_t> positions,int64_t windows_gap);
    int64_t find();
    static int64_t roundFloorToDay(int64_t timestamp);
private:
    TimestampArray* step_time_stamps;
    int max_row;
    std::vector<int64_t> positions;
    std::vector<int64_t> limit_from_step0;
    int64_t limit_max;
    bool visit_any_step_max_valid_timestamp;
    int current_step = -1;

    bool moveCurrentStepPositionAndAdjust();
    bool advance_step();
    void adjustPrevious(int step);
};

MaxStepFinder::MaxStepFinder(TimestampArray* stepTimestamps,int maxRow,std::vector<int64_t> positions,int64_t windows_gap) {
    this->step_time_stamps = stepTimestamps;
    this->max_row = maxRow;
    this->positions = std::move(positions);
    this->visit_any_step_max_valid_timestamp = false;

    const TimestampArray step0_timestamps = stepTimestamps[0];
    int length = step0_timestamps.size;
    std::vector<int64_t> limit(length,0);
    for (int i = 0; i < length; i++) {
        int64_t start_time = step0_timestamps[i];
        if (windows_gap>0){
            limit[i] = start_time+windows_gap;
        } else if (windows_gap==-1){
            int64_t start_time_trunc_day= roundFloorToDay(start_time);
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
            adjustPrevious(this->current_step);
        }
        if(this->current_step==this->max_row){
            break;
        }
    } while (moveCurrentStepPositionAndAdjust());
    return this->current_step;
}

bool MaxStepFinder::moveCurrentStepPositionAndAdjust(){
    while (!this->visit_any_step_max_valid_timestamp){
        TimestampArray timestamps = this->step_time_stamps[this->current_step];
        int p = this->positions[this->current_step];
        if (p<timestamps.size-1){
            throw std::runtime_error(fmt::format("already visited the max timestamp of step {}" , this->current_step));
        }
        this->positions[this->current_step]= p +1;
        int64_t current_time_stamp = this->step_time_stamps[this->current_step][this->positions[this->current_step]];
        if(p+1 == timestamps.size-1||current_time_stamp>=this->limit_max){
            this->visit_any_step_max_valid_timestamp = true;
            if (current_time_stamp>this->limit_max){
                break;
            }
        }
        adjustPrevious(this->current_step);
        int64_t limit0=this->limit_from_step0[this->positions[0]];
        if (current_time_stamp<limit0){
            return true;
        }
    }
    return false;
}

bool MaxStepFinder::advance_step(){
    int step = this->current_step+1;
    if (step > this->max_row){
        return false;
    }
    TimestampArray timestamp = this->step_time_stamps[step];
    int p = this->positions[step];
    if (p < timestamp.size-1){
        throw std::runtime_error(fmt::format("already visited the max timestamp of step {} " , step));
    }
    int np;
    if (step > 0){
        int64_t current_time_stamp = this->step_time_stamps[this->current_step][this->positions[this->current_step]];
        int maxLe = p;
        while(maxLe+1<timestamp.size && timestamp[maxLe+1]<=current_time_stamp){
            maxLe++;
        }
        this->positions[step]=maxLe;
        if (maxLe == timestamp.size-1){
            this->visit_any_step_max_valid_timestamp = true;
            np = maxLe;
        }else{
            int64_t limit0 = this->limit_from_step0[this->positions[0]];
            int64_t timestamp0 = timestamp[maxLe];
            if (timestamp0 < limit0){
                np = maxLe+1;
            }else{
                np=maxLe;
                if (timestamp0>= this->limit_max){
                    this->visit_any_step_max_valid_timestamp = true;
                }
            }
        }
    }else{
        np = p +1;
    }
    if (np>this->positions[step]){
        this->current_step=step;
        this->positions[step]=np;
        if (np ==timestamp.size-1){
            this->visit_any_step_max_valid_timestamp= true;
        }
        return true;
    }else{
        return false;
    }
}

void MaxStepFinder::adjustPrevious(int step){
    while(step>0){
        int64_t time = this->step_time_stamps[step][this->positions[step]];
        int pStep = step-1;
        TimestampArray pStepTimestamp = this->step_time_stamps[pStep];
        int pStepIndex = this->positions[pStep];
        while (pStepIndex+1<pStepTimestamp.size&&pStepTimestamp[pStepIndex+1]<time){
            pStepIndex++;
        }
        if (pStepIndex>this->positions[pStep]){
            this->positions[pStep]=pStepIndex;
            if (pStepIndex==pStepTimestamp.size-1){
                this->visit_any_step_max_valid_timestamp=true;
            }
            step=pStep;
        }else{
            break;
        }
    }
}
int64_t MaxStepFinder::roundFloorToDay(int64_t timestamp) {
    std::time_t time = timestamp / 1000; // Convert milliseconds to seconds
    std::tm* tm = std::gmtime(&time); // Convert to UTC time structure
    tm->tm_hour = 0;
    tm->tm_min = 0;
    tm->tm_sec = 0;
    std::time_t startOfDay = std::mktime(tm);
    return startOfDay * 1000;
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
        int64_t packed_time_millis = array.value(j);
        int32_t funnel_index = int32_t(packed_time_millis & FUNNEL_INDEX_MASK);
        if (funnel_index < 1 || funnel_index > max_funnel_index) {
            continue;
        }
        size[funnel_index]++;
    }
    for (int i = 0; i < max_funnel_index; i++) {
        stepTimestamps[i].elements = timestamps;
        timestamps+=size[i];
    }
    for (size_t j = start; j < end; j++) {
        int64_t packed_time_millis = array.value(j);
        size_t funnel_index = size_t(packed_time_millis & FUNNEL_INDEX_MASK);
        if (funnel_index < 1 || funnel_index > max_funnel_index) {
            continue;
        }
        size_t f = funnel_index - 1;
        stepTimestamps[f].add(int64_t(packed_time_millis >> MILLIS_SHIFT));
    }

    for (size_t i = 0; i < max_funnel_index; i++) {
        if (!stepTimestamps[i].empty()){
            std::sort(stepTimestamps[i].begin(), stepTimestamps[i].end());
        }
    }
    return;
}

static int64_t funnel_max_step_inner(const ColumnViewer<TYPE_BIGINT>& array,
                              size_t start,
                              size_t end,
                              int64_t windows_gap,
                              int64_t max_funnel_index,
                              int64_t* timestamps,
                              TimestampArray* stepTimestamps) {
    if (end - start == 0) {
        return 0;
    }
    toMatrix(array, start, end,timestamps,stepTimestamps, max_funnel_index);
    int maxKey = -1;
    while(maxKey+1<max_funnel_index && !stepTimestamps[maxKey + 1].empty()) {
        maxKey++;
    }
    if (maxKey<=0){
        return maxKey+1;
    }
    std::vector<int64_t> positions (maxKey+1,-1);
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
        int64_t mills = unix_timestamp / 1000;
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
    auto windows_viewer = ColumnViewer<TYPE_BIGINT>(columns[1]);
    auto maxFunnelIndex_viewer = ColumnViewer<TYPE_INT>(columns[2]);
    const size_t num_rows = array_offsets.size();

    ColumnBuilder<TYPE_BIGINT> builder(num_rows);
    // TODO: pre-allocation a large vector
    size_t max_elements = 0;
    for (size_t i = 0; i < num_rows-1; i++) {
        size_t start = array_offsets.get_data()[i];
        size_t end = array_offsets.get_data()[i+1];
        max_elements = std::max(max_elements, end - start);
    }

    // Global timestamp container allow the whole function to share the same memory pool

    gscoped_array<int64_t> timestamps(new int64_t[max_elements]);

    for (size_t i = 0; i < num_rows-1; i++) {
        size_t start = array_offsets.get_data()[i];
        size_t end = array_offsets.get_data()[i+1];

        int64_t funnel_count = maxFunnelIndex_viewer.value(i);
        if (funnel_count > 128) {
            throw std::runtime_error(fmt::format("funnelIndex overflow: {}", funnel_count));
        }
        // Each step has a timestamp array, and each timestamp array has a size
        TimestampArray stepTimestamps [funnel_count];
        memset(stepTimestamps, 0, sizeof(TimestampArray)*funnel_count);

        int64_t funnel_max_step = funnel_max_step_inner(
                viewer,
                start,
                end,
                windows_viewer.value(i),
                funnel_count,
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

    auto viewer = ColumnViewer<TYPE_BIGINT>(col_array->elements_column());

    auto windows_gap_viewer = ColumnViewer<TYPE_BIGINT>(columns[1]);
    auto funnel_index_viewer = ColumnViewer<TYPE_INT>(columns[2]);
    auto start_time_viewer = ColumnViewer<TYPE_DATETIME>(columns[3]);
    auto end_time_viewer = ColumnViewer<TYPE_DATETIME>(columns[4]);

    const size_t num_rows = array_offsets.size();

    size_t max_elements = 0;
    size_t max_funnel_index = 0;
    for (size_t i = 0; i < num_rows-1; i++) {
        size_t start = array_offsets.get_data()[i];
        size_t end = array_offsets.get_data()[i+1];
        max_elements = std::max(max_elements, end - start);
    }

    gscoped_array<int64_t> timestamps(new int64_t[max_elements]);

    ColumnBuilder<TYPE_DATETIME> key_builder(num_rows);
    ColumnBuilder<TYPE_BIGINT> value_builder(num_rows);
    UInt32Column::Ptr new_offsets = UInt32Column::create();
    new_offsets->reserve(num_rows + 1);
    auto& offsets_vec = new_offsets->get_data();
    offsets_vec.push_back(0);

    for (size_t i = 0; i < num_rows - 1 ; ++i) {
        size_t start = array_offsets.get_data()[i];
        size_t end = array_offsets.get_data()[i+1];
        int32_t funnel_count = funnel_index_viewer.value(i);
        if (funnel_count > 128) {
            throw std::runtime_error(fmt::format("funnelIndex overflow: {}", max_funnel_index));
        }

        int64_t windows_gap = windows_gap_viewer.value(i);
        TimestampValue start_time = start_time_viewer.value(i);
        TimestampValue end_time = end_time_viewer.value(i);
        TimestampArray stepTimestamps [funnel_count];
        memset(stepTimestamps, 0, sizeof(TimestampArray)*funnel_count);
        phmap::flat_hash_map<int64_t, int64_t> result =funnel_max_step_date_inner(viewer,
                                   start,
                                   end,
                                   windows_gap,
                   funnel_count,
                                   start_time,
                                   end_time,
                                   timestamps.get(),
                                   stepTimestamps);
        for(auto& [key,value]:result){
            long unixSeconds = key / 1000;
            long unixMillis = (key % 1000)* 1000;
            TimestampValue timestampValue;
            timestampValue.from_unixtime(unixSeconds,unixMillis,cctz::utc_time_zone());
            key_builder.append(timestampValue);
            value_builder.append(value);
        }
        new_offsets->append(result.size());
    }
    return MapColumn::create(key_builder.build(ColumnHelper::is_all_const(columns)),
                             value_builder.build(ColumnHelper::is_all_const(columns)),
                             new_offsets);
}
int64_t microsToMillis(int64_t micros) {
    return micros / 1000;
}
int adjustStartPositions(TimestampArray* stepTimestamps, std::vector<int64_t>& positions, int maxKey);
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
        int64_t key = microsToMillis(start_time.to_unix_microsecond());
        int64_t end_timestamp = microsToMillis(end_time.to_unix_microsecond());
        while (key <= end_timestamp) {
            resultMap[key]=0L;
            key = key + DAY_GAP_MILLS;
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

    std::vector<int64_t> positions(maxKey+1, -1);
    phmap::flat_hash_map<int64_t ,std::vector<int64_t>> step0map;
    for (int i = 0; i < stepTimestamps[0].size; i++) {
        int64_t group = MaxStepFinder::roundFloorToDay(stepTimestamps[0][i]);
        step0map[group].push_back(i);
    }
    for (auto& [key,value] : step0map) {
        int32_t maxStep;
        if (maxKey<=0) {
            maxStep=1;
        }else{
            stepTimestamps[0] = TimestampArray{value.data(),value.size()};
            maxKey = adjustStartPositions(stepTimestamps, value, maxKey);
            std::vector<int64_t> copy(positions.begin(), positions.begin() + maxKey + 1);
            maxStep = MaxStepFinder(stepTimestamps, maxKey,copy, windows_gap).find() + 1;
        }
        resultMap[key]=maxStep;
    }
    return resultMap;

}

int adjustStartPositions(TimestampArray* stepTimestamps, std::vector<int64_t>& positions, int maxKey) {
    int32_t start_time = stepTimestamps[0][0];
    for (int step = 1; step <= maxKey; step++) {
        auto timestamps = stepTimestamps[step];
        int p = positions[step];
        while (p < timestamps.size && timestamps[p] < start_time) {
            p++;
        }
        if (p==timestamps.size-1){
            return -1;
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



} // namespace starrocks