//
// Created by Co1a on 24-11-7.
//

#pragma once

#include "column/column_viewer.h"
#include "column/column_helper.h"
#include "column/array_column.h"
#include "column/fixed_length_column.h"

namespace starrocks {
// Function: funnel_flow_array
struct IntArrayState{
    void init(int64_t totalStep){
        this->_data=std::vector<int32_t>(totalStep,0);
        this->_init = true;
    }
    void reset(){
        this->_data.clear();
        this->_init = false;
    }
    void update(const ColumnViewer<TYPE_BIGINT>& column, size_t start,size_t end){
        for (size_t i=0;i<_data.size();i++){
            this->_data[i] += column.value(i+start);
        }
    }
    bool _init = false;
    std::vector<int32_t> _data = {};
};

class FunnelFlowArrayAggregateFunction final
        : public AggregateFunctionBatchHelper<IntArrayState, FunnelFlowArrayAggregateFunction> {
public:
    void init_state_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state)const{
        if (this->data(state)._init){
            return;
        }
        size_t constant_num = ctx->get_num_constant_columns();
        auto totalStep = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(constant_num-1));
        this->data(state).init(totalStep);
    }

    void reset (FunctionContext* ctx, const Columns& arg,AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()){
            return;
        }
        init_state_if_necessary(ctx, state);
        auto elements = ColumnViewer<TYPE_BIGINT>(columns[0]->clone_shared());
        for (size_t j = 0; j < elements.value(row_num); j++){
            this->data(state)._data[j] += 1;
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        init_state_if_necessary(ctx, state);
        const auto* input_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column));
        auto offset = input_column->offsets();
        auto elements = ColumnViewer<TYPE_BIGINT>(input_column->elements_column());
        for(size_t i = 0;i < offset.size()-1; i++){
            auto start = offset.get_data()[i];
            auto end = offset.get_data()[i+1];
            this->data(state).update(elements,start,end);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto dst = down_cast<ArrayColumn*>(to);
        size_t size = data(state)._data.size();
        for (int i = 0; i < size; ++i) {
            dst->elements_column()->append_datum(data(state)._data[i]);
        }
        dst->offsets_column()->append(size);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        auto offset_column = array_column->offsets_column();
        size_t elem_size = data(state)._data.size();
        for (int i = 0; i < elem_size; ++i) {
            array_column->elements_column()->append_datum(data(state)._data[i]);
        }
        offset_column->append(elem_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<ArrayColumn*>((*dst).get());
        dst_column->reserve(chunk_size);

        auto elements = ColumnViewer<TYPE_BIGINT>(src[0]->clone_shared());

        int constant_num = ctx->get_num_constant_columns();
        auto total_step = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(constant_num - 1));

        for (size_t i = 0; i < chunk_size; ++i) {
            auto value = elements.value(i);
            for (int j = 0; j < total_step; j++){
                if (j < value){
                    dst_column->elements_column()->append_datum(1);
                }else{
                    dst_column->elements_column()->append_datum(0);
                }
            }
            dst_column->offsets_column()->append_datum(i+1*total_step);
        }
    }

    std::string get_name() const override { return "funnel_flow_array"; }
};

// Function: funnel_packed_time_collect


static const int64_t DAY_GAP_MILLS = 86400000L;
static const int64_t FUNNEL_INDEX_MASK = 0xFFF;
static const int32_t MILLIS_SHIFT = 12;

struct FunnelPackedTimeCollectState{
    const static size_t DEFAULT_ARRAY_SIZE = 16;
    void init(){
        this->_data = std::vector<int64_t>(DEFAULT_ARRAY_SIZE);
        this->_init = true;
    }
    void reset(){
        this->_data.clear();
    }
    void update(int64_t timestamp, int64_t funnel_index_bitset, int64_t funnel_index_start){
        for (int i =0;i<64;i++){
            auto shifted = funnel_index_bitset >> i;
            if(shifted==0){
                break;
            }
            if((shifted&1)==1){
                auto funnel_packed_time = funnel_pack_time(timestamp,funnel_index_start+i);
                this->_data.push_back(funnel_packed_time);
            }
        }
    }
    void insert(int64_t funnel_packed_time){
        this->_data.push_back(funnel_packed_time);
    }
    std::vector<int64_t> _data = {};
    bool _init = false;

    static int64_t funnel_pack_time(int64_t timestamp,int32_t funnel_index){
        if (funnel_index > FUNNEL_INDEX_MASK) {
            throw std::runtime_error("funnelIndex overflow: " + std::to_string(funnel_index));
        }
        int64_t unix_timestamp = timestamp;
        int64_t mills = unix_timestamp / 1000;
        int64_t shiftedMills = mills << MILLIS_SHIFT;
        if (shiftedMills >> MILLIS_SHIFT != mills) {
            throw std::runtime_error("Millis overflow: " + std::to_string(shiftedMills));
        }
        return shiftedMills | (funnel_index & FUNNEL_INDEX_MASK);
    }
};

class FunnelPackedTimeCollectAggregateFunction
        : public AggregateFunctionBatchHelper<FunnelPackedTimeCollectState, FunnelPackedTimeCollectAggregateFunction> {
private:
    static int64_t funnel_pack_time(int64_t timestamp,int32_t funnel_index){
        if (funnel_index > FUNNEL_INDEX_MASK) {
            throw std::runtime_error("funnelIndex overflow: " + std::to_string(funnel_index));
        }
        int64_t unix_timestamp = timestamp;
        int64_t mills = unix_timestamp / 1000;
        int64_t shiftedMills = mills << MILLIS_SHIFT;
        if (shiftedMills >> MILLIS_SHIFT != mills) {
            throw std::runtime_error("Millis overflow: " + std::to_string(shiftedMills));
        }
        return shiftedMills | (funnel_index & FUNNEL_INDEX_MASK);
    }
public:
    void init_if_necessary([[maybe_unused]]FunctionContext* ctx, AggDataPtr __restrict state)const{
        if (this->data(state)._init){
            return;
        }
        this->data(state).init();
    }
    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,ColumnPtr* dst) const override {
        auto* dst_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(dst->get()));
        dst_column->reserve(chunk_size);
        auto int_column = down_cast<Int64Column*>(dst_column->elements_column().get());
        auto datetime_viewer = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(src[0].get()));
        auto funnel_index_bitset = down_cast<const Int64Column*>(ColumnHelper::get_data_column(src[1].get()));
        size_t element_size = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            auto datetime = datetime_viewer->get(i).get_timestamp();
            auto funnel_index = funnel_index_bitset->get(i).get_int64();
            if (datetime_viewer->is_null(i) || funnel_index_bitset->is_null(i)){
                dst_column->append_nulls(1);
                continue;
            }
            auto timestamp = datetime.to_unix_microsecond();
            for (int j = 0; j < 64; j++){
                auto shifted = funnel_index >> j;
                if(shifted==0){
                    break;
                }
                if((shifted&1)==1){
                    auto funnel_packed_time = funnel_pack_time(timestamp,j);
                    int_column->append(funnel_packed_time);
                    element_size++;
                }
            }
            dst_column->offsets_column()->append(element_size);
        }
    }
    void reset (FunctionContext* ctx, const Columns& arg,AggDataPtr state) const override {
        this->data(state).reset();
    }
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,size_t row_num) const override {
        if (((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null())
                ||
            ((columns[1]->is_nullable() && columns[1]->is_null(row_num))|| columns[1]->only_null())){
            return;
        }
        init_if_necessary(ctx, state);

        auto datetime_viewer = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(columns[0]));
        auto funnel_index_bitset = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[1]));

        this->data(state).update(
                datetime_viewer->get_data()[row_num].to_unix_microsecond(),
                funnel_index_bitset->get_data()[row_num],1);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        init_if_necessary(ctx, state);
        auto* input_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column));
        auto offset = input_column->offsets();
        auto elements = down_cast<Int64Column*>(ColumnHelper::get_data_column(input_column->elements_column().get()));
        size_t begin = offset.get_data()[row_num];
        size_t end = offset.get_data()[row_num+1];
        for(size_t i = begin;i < end; i++){
            this->data(state).insert(elements->get_data()[i]);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }
        auto dst = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        size_t size = this->data(state)._data.size();

        NullableColumn* pNullableColumn;
        if (dst->elements_column()->is_nullable()){
            pNullableColumn = down_cast<NullableColumn*>(dst->elements_column().get());
        }
        auto int_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst->elements_column().get()));
        for(size_t i=0 ;i < size; i++){
            pNullableColumn->null_column()->append(0);
        }
        int_column->get_data().insert(int_column->get_data().end(),data(state)._data.begin(),data(state)._data.end());
        dst->offsets_column()->append(dst->offsets_column()->get_data().back()+size);
    }
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }

        auto dst = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        NullableColumn* pNullableColumn;
        if (dst->elements_column()->is_nullable()){
            pNullableColumn = down_cast<NullableColumn*>(dst->elements_column().get());
        }
        size_t elem_size = data(state)._data.size();
        auto int_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst->elements_column().get()));
        for (int i = 0; i < elem_size; ++i) {
            pNullableColumn->null_column()->append(0);
        }
        int_column->get_data().insert(int_column->get_data().end(),data(state)._data.begin(),data(state)._data.end());
        dst->offsets_column()->append(dst->offsets_column()->get_data().back()+elem_size);
    }
    std::string get_name() const override { return "funnel_packed_time_collect"; }
};

// Function: funnel_packed_time_collect2
class FunnelPackedTimeCollectAggregateFunction2
        : public AggregateFunctionBatchHelper<FunnelPackedTimeCollectState, FunnelPackedTimeCollectAggregateFunction> {
private:
    static int64_t funnel_pack_time(int64_t timestamp,int32_t funnel_index){
        if (funnel_index > FUNNEL_INDEX_MASK) {
            throw std::runtime_error("funnelIndex overflow: " + std::to_string(funnel_index));
        }
        int64_t unix_timestamp = timestamp;
        int64_t mills = unix_timestamp / 1000;
        int64_t shiftedMills = mills << MILLIS_SHIFT;
        if (shiftedMills >> MILLIS_SHIFT != mills) {
            throw std::runtime_error("Millis overflow: " + std::to_string(shiftedMills));
        }
        return shiftedMills | (funnel_index & FUNNEL_INDEX_MASK);
    }
public:
    void init_if_necessary([[maybe_unused]]FunctionContext* ctx, AggDataPtr __restrict state)const{
        if (this->data(state)._init){
            return;
        }
        this->data(state).init();
    }
    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,ColumnPtr* dst) const override {
        auto* dst_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(dst->get()));
        dst_column->reserve(chunk_size);
        auto int_column = down_cast<Int64Column*>(dst_column->elements_column().get());
        auto datetime_viewer = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(src[0].get()))->get_data();
        auto funnel_index_bitset = down_cast<const Int64Column*>(ColumnHelper::get_data_column(src[1].get()))->get_data();
        auto funnel_index_bitset2 = down_cast<const Int64Column*>(ColumnHelper::get_data_column(src[2].get()))->get_data();
        size_t element_size = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            auto datetime = datetime_viewer[i];
            auto funnel_index = funnel_index_bitset[i];
            auto funnel_index2 = funnel_index_bitset2[i];
            auto timestamp = datetime.to_unix_microsecond();
            for (int j = 0; j < 64; j++){
                auto shifted = funnel_index >> j;
                if(shifted==0){
                    break;
                }
                if((shifted&1)==1){
                    auto funnel_packed_time = funnel_pack_time(timestamp,j);
                    int_column->append(funnel_packed_time);
                    element_size++;
                }
            }
            for (int j = 0; j < 64; j++){
                auto shifted = funnel_index2 >> j;
                if(shifted==0){
                    break;
                }
                if((shifted&1)==1){
                    auto funnel_packed_time = funnel_pack_time(timestamp,j);
                    int_column->append(funnel_packed_time);
                    element_size++;
                }
            }
            dst_column->offsets_column()->append(element_size);
        }
    }
    void reset (FunctionContext* ctx, const Columns& arg,AggDataPtr state) const override {
        this->data(state).reset();
    }
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,size_t row_num) const override {
        if (((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null())
            ||
            ((columns[1]->is_nullable() && columns[1]->is_null(row_num))|| columns[1]->only_null())){
            return;
        }
        init_if_necessary(ctx, state);

        auto datetime_viewer = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(columns[0]))->get_data();
        auto funnel_index_bitset = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[1]))->get_data();
        auto funnel_index_bitset2 = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[2]))->get_data();


        this->data(state).update(
                datetime_viewer[row_num].to_unix_microsecond(),
                funnel_index_bitset[row_num],1);
        this->data(state).update(
                datetime_viewer[row_num].to_unix_microsecond(),
                funnel_index_bitset2[row_num],1+64);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        init_if_necessary(ctx, state);
        auto* input_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column));
        auto offset = input_column->offsets();
        auto elements = down_cast<Int64Column*>(ColumnHelper::get_data_column(input_column->elements_column().get()));
        size_t begin = offset.get_data()[row_num];
        size_t end = offset.get_data()[row_num+1];
        for(size_t i = begin;i < end; i++){
            this->data(state).insert(elements->get_data()[i]);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }
        auto dst = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        size_t size = this->data(state)._data.size();

        NullableColumn* pNullableColumn;
        if (dst->elements_column()->is_nullable()){
            pNullableColumn = down_cast<NullableColumn*>(dst->elements_column().get());
        }
        auto int_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst->elements_column().get()));
        for(size_t i=0 ;i < size; i++){
            pNullableColumn->null_column()->append(0);
        }
        int_column->get_data().insert(int_column->get_data().end(),data(state)._data.begin(),data(state)._data.end());
        dst->offsets_column()->append(dst->offsets_column()->get_data().back()+size);
    }
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }

        auto dst = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        NullableColumn* pNullableColumn;
        if (dst->elements_column()->is_nullable()){
            pNullableColumn = down_cast<NullableColumn*>(dst->elements_column().get());
        }
        size_t elem_size = data(state)._data.size();
        auto int_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst->elements_column().get()));
        for (int i = 0; i < elem_size; ++i) {
            pNullableColumn->null_column()->append(0);
        }
        int_column->get_data().insert(int_column->get_data().end(),data(state)._data.begin(),data(state)._data.end());
        dst->offsets_column()->append(dst->offsets_column()->get_data().back()+elem_size);
    }
    std::string get_name() const override { return "funnel_packed_time_collect"; }
};

// Function: funnel_flow_array_date

struct FunnelFlowArrayDate{
    void init(FunctionContext* ctx,size_t map_size){
        if (!this->_init){
            return;
        }
        int constant_num = ctx->get_num_constant_columns();
        auto total_step = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(constant_num - 1));
        this->_total_step = total_step;
        // The whole element size is total_step * map_size
        size_t vector_size = (total_step+1) * map_size;
        this->_data = Int64Column::create_mutable();
        this->_data->reserve(vector_size);
        // Finish offset Column Init
        this->_offsets = UInt32Column::create_mutable();
        this->_offsets->resize(map_size+1);
        this->_offsets->append_default();
        this->_init = true;
    }

    void reset(){
        this->_data.reset();
        this->_offsets.reset();
        this->_init = false;
        this->_total_step = 0;
    }

    void update(const TimestampColumn* key_column,
                const Int64Column* value_column,
                size_t start,size_t end){
        auto& data = down_cast<Int64Column*>(this->_data.get())->get_data();
        auto& offsets = down_cast<UInt32Column*>(this->_offsets.get())->get_data();
        for (size_t i = start; i < end; ++i) {
            auto date_time = key_column->get_data()[i].to_unix_microsecond();
            auto max_step = value_column->get_data()[i];
            data.emplace_back(date_time);
            for (int j = start+1; j < max_step; j++){
                data.emplace_back(1);
            }
        }
        offsets.push_back(data.size());
    }

    void merge(const Int64Column* elements, size_t start, size_t end){
        auto& data = down_cast<Int64Column*>(this->_data.get())->get_data();
        const auto& elements_data = elements->get_data();
        for (size_t i = start+1; i < end; ++i) {
            data[i] += elements_data[i];
        }
    }

    ColumnPtr _data = {};
    ColumnPtr _offsets = {};
    int64_t _total_step = {};
    bool _init = false;
};


class FunnelFlowArrayDateAggregateFunction
        : public AggregateFunctionBatchHelper<FunnelFlowArrayDate, FunnelFlowArrayDateAggregateFunction> {
    void init_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state,size_t map_size) const{
        if (this->data(state)._init){
            return;
        }
        this->data(state).init(ctx,map_size);
    }

public:
    void reset(FunctionContext* ctx, const Columns& arg,AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        LOG(INFO)<< "Update";
        if((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()){
            return;
        }
        auto elements = down_cast<const MapColumn*>(ColumnHelper::get_data_column(columns[0]));
        auto map_size = elements->size();
        init_if_necessary(ctx, state,map_size);
        auto keys = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(elements->keys_column().get()));
        auto values = down_cast<const Int64Column*>(ColumnHelper::get_data_column(elements->values_column().get()));
        auto offset = elements->offsets();
        this->data(state).update(keys,values,
                                 offset.get_data()[row_num],offset.get_data()[row_num+1]);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        LOG(INFO)<< "Merge";
        const auto* input_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column));
        init_if_necessary(ctx, state,1);
        auto offset = input_column->offsets();
        auto elements = down_cast<const Int64Column*>(ColumnHelper::get_data_column(input_column->elements_column().get()));
        auto start = offset.get_data()[row_num];
        auto end = offset.get_data()[row_num+1];
        this->data(state).merge(elements,start,end);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        LOG(INFO)<< "Serialize to column";
        if (to->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }
        auto* dst_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        auto data = down_cast<Int64Column*>(dst_column->elements_column().get())->get_data();
        data.reserve(this->data(state)._data->size());
        LOG(INFO)<< "Data size: "<<this->data(state)._data->size();
        auto original_data = down_cast<Int64Column*>(this->data(state)._data.get())->get_data();
        data.insert(data.end(),original_data.begin(),original_data.end());

        auto offset = down_cast<UInt32Column*>(dst_column->elements_column().get())->get_data();
        auto original_offset = down_cast<UInt32Column*>(this->data(state)._offsets.get())->get_data();
        offset.reserve(original_offset.size());
        offset.insert(offset.end(),original_offset.begin(),original_offset.end());
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        LOG(INFO)<< "Finalize to column";
        if (to->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }
        auto* map_column = down_cast<MapColumn*>(ColumnHelper::get_data_column(to));
        if (map_column->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }

        auto key_column = down_cast<TimestampColumn*>(ColumnHelper::get_data_column(map_column->keys_column().get()));
        if (key_column->is_nullable()){
            auto dst = down_cast<NullableColumn*>(map_column->keys_column().get());
            dst->null_column()->append(0);
        }

        // QUESTION: This value column should be ArrayColumn or StructColumn?
        auto value_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(map_column->values_column().get()));
        if (value_column->is_nullable()){
            auto dst = down_cast<NullableColumn*>(map_column->values_column().get());
            dst->null_column()->append(0);
        }

        auto& offsets = map_column->offsets_column()->get_data();
        // Init the offset

        auto data_viewer = down_cast<Int64Column*>(data(state)._data.get())->get_data();
        auto offset_viewer = down_cast<UInt32Column*>(data(state)._offsets.get())->get_data();

        offsets.reserve(offset_viewer.size());
        for (size_t i=0;i<offset_viewer.size()-1;i++){
            // Get the start and end index of the data
            auto start = offset_viewer[i];
            auto end = offset_viewer[i+1];
            // Append the key
            TimestampValue key = TimestampValue::create_from_unixtime(data_viewer[start],cctz::utc_time_zone());
            key_column->append(key);
            // Append the value
            key_column->append(key);
            DatumArray array;
            LOG(INFO)<< "Data size: "<<end - start;
            array.reserve(end-start-1);
            array.insert(array.end(),data_viewer.begin()+start+1,data_viewer.begin()+end);
            value_column->append_datum(Datum(array));
            offsets.push_back(i);
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        LOG(INFO)<< "Convert to serialize format";
        auto* dst_column = down_cast<ArrayColumn*>((*dst).get());
        dst_column->reserve(chunk_size);

        auto map_column = down_cast<MapColumn*>(src[0].get());
        auto keys = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(map_column->keys_column().get()))
                            ->get_data();
        auto values = down_cast<const Int64Column*>(ColumnHelper::get_data_column(map_column->values_column().get()))
                              ->get_data();
        auto offset = map_column->offsets_column();

        int constant_num = ctx->get_num_constant_columns();
        auto total_step = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(constant_num - 1));

        dst_column->elements_column()->reserve(chunk_size*total_step+1);

        auto element_column = down_cast<Int64Column*>(dst_column->elements_column().get())->get_data();

        for (size_t i = 0; i < chunk_size; ++i) {
            auto key = keys[i];
            auto value = values[i];
            auto start = offset->get_data()[i];
            auto end = offset->get_data()[i + 1];
            for (size_t j = start; j < end; ++j) {
                auto date_time = key.to_unix_microsecond();
                auto max_step = value;
                element_column.emplace_back(date_time);
                for (int k = 1; k <= max_step; k++){
                    element_column.emplace_back(1);
                }
            }
        }
    }
    std::string get_name()const override{return "funnel_flow_array_date";}
};
}