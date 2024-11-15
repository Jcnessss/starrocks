//
// Created by Co1a on 24-11-7.
//

#pragma once

#include "column/column_viewer.h"
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

}