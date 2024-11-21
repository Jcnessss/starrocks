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
        this->_data.resize(totalStep);
        this->_init = true;
    }
    void reset(){
        this->_data.clear();
        this->_init = false;
    }
    void update(const Int64Column* column, size_t start,size_t end){
        auto data_column = column->get_data();
        for (size_t i=0;i<_data.size();i++){
            this->_data[i] += data_column[i+start];
        }
    }
    bool _init = false;
    std::vector<int64_t> _data = {};
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
        auto elements = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[0]))->get_data();
        for (size_t j = 0; j < elements[row_num]; j++){
            this->data(state)._data[j] += 1;
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        init_state_if_necessary(ctx, state);
        const auto* input_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column));
        auto offset = input_column->offsets();
        auto elements = down_cast<const Int64Column*>(ColumnHelper::get_data_column(input_column->elements_column().get()));
        for(size_t i = 0;i < this->data(state)._data.size(); i++){
            this->data(state)._data[i] += elements->get_data()[offset.get_data()[row_num]+i];
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto dst = down_cast<ArrayColumn*>(to);
        if (to->is_nullable()&& this->data(state)._data.size()==0) {
            auto null_column = down_cast<NullableColumn*>(to)->null_column();
            null_column->append(1);
            return;
        }
        size_t size = data(state)._data.size();
        auto data_column  = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst->elements_column().get()));
        if (dst->elements().is_nullable()) {
            auto null_column = down_cast<NullableColumn*>(dst->elements_column().get())->null_column();
            null_column->append(0);
        }
       data_column->get_data().insert(data_column->get_data().end(),data(state)._data.begin(),data(state)._data.end());
        dst->offsets_column()->append(dst->offsets_column()->get_data().back()+size);
        if (to->is_nullable()){
            auto null_column = down_cast<NullableColumn*>(to)->null_column();
            null_column->append(0);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        if (to->is_nullable()&& this->data(state)._data.size()==0) {
            auto null_column = down_cast<NullableColumn*>(to)->null_column();
            null_column->append(1);
            return;
        }
        auto data_column  = down_cast<Int64Column*>(ColumnHelper::get_data_column(array_column->elements_column().get()));
        if (array_column->elements().is_nullable()) {
            auto null_column = down_cast<NullableColumn*>(array_column->elements_column().get())->null_column();
            null_column->get_data().insert(null_column->get_data().end(),data(state)._data.size(),0);
        }
        auto offset_column = array_column->offsets_column();
        size_t elem_size = data(state)._data.size();
        data_column->get_data().insert(data_column->get_data().end(),data(state)._data.begin(),data(state)._data.end());
        offset_column->append(offset_column->get_data().back()+elem_size);
        if (to->is_nullable()){
            auto null_column = down_cast<NullableColumn*>(to)->null_column();
            null_column->append(0);
        }
        offset_column->append(offset_column->get_data().back()+elem_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<ArrayColumn*>((*dst).get());
        dst_column->reserve(chunk_size);

        auto elements = down_cast<const Int64Column*>(ColumnHelper::get_data_column(src[0].get()))->get_data();

        int constant_num = ctx->get_num_constant_columns();
        auto total_step = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(constant_num - 1));

        auto dst_element_cloumn = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst_column->elements_column().get()));

        for (size_t i = 0; i < chunk_size; ++i) {
            auto value = elements[i];
            for (int j = 0; j < total_step; j++){
                if (j < value){
                    dst_element_cloumn->append_datum(1);
                }else{
                    dst_element_cloumn->append_datum(0);
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
        // this->_data.reserve(DEFAULT_ARRAY_SIZE);
        this->_init = true;
    }
    void reset(){
        this->_data.clear();
        this->_init = false;
    }
    void update(const int64_t timestamp,const int64_t funnel_index_bitset,const int64_t funnel_index_start){
        for (int i =0;i<64;i++){
            const auto shifted = (funnel_index_bitset|0U) >> i;
            if(shifted==0){
                break;
            }
            if((shifted&1)==1){
                auto funnel_packed_time = funnel_pack_time(timestamp,funnel_index_start+i);
                this->_data.emplace_back(funnel_packed_time);
            }
        }
    }
    void insert(const int64_t funnel_packed_time){
        this->_data.emplace_back(funnel_packed_time);
    }
    std::vector<int64_t> _data = {};
    bool _init = false;

    static int64_t funnel_pack_time(const int64_t timestamp,const int32_t funnel_index){
        if (funnel_index > FUNNEL_INDEX_MASK) {
            throw std::runtime_error("funnelIndex overflow: " + std::to_string(funnel_index));
        }
        const int64_t unix_timestamp = timestamp;
        const int64_t mills = std::floor(unix_timestamp / 1000);
        const int64_t shiftedMills = mills << MILLIS_SHIFT;
        if (shiftedMills >> MILLIS_SHIFT != mills) {
            throw std::runtime_error("Millis overflow: " + std::to_string(shiftedMills));
        }
        return shiftedMills | (funnel_index & FUNNEL_INDEX_MASK);
    }
};

class FunnelPackedTimeCollectAggregateFunction
        : public AggregateFunctionBatchHelper<FunnelPackedTimeCollectState, FunnelPackedTimeCollectAggregateFunction> {
private:
    static int64_t funnel_pack_time(const int64_t timestamp, const int32_t funnel_index){
        if (funnel_index > FUNNEL_INDEX_MASK) {
            throw std::runtime_error("funnelIndex overflow: " + std::to_string(funnel_index));
        }
        const int64_t unix_timestamp = timestamp;
        const int64_t mills = std::floor(unix_timestamp / 1000);
        const int64_t shiftedMills = mills << MILLIS_SHIFT;
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
        size_t element_size = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            auto datetime = datetime_viewer[i];
            auto const funnel_index = funnel_index_bitset[i];
            auto const timestamp = datetime.to_unix_microsecond();
            for (int j = 0; j < 64; j++){
                const auto shifted = (funnel_index|0U) >> j;
                if(shifted==0){
                    break;
                }
                if((shifted&1)==1){
                    const auto funnel_packed_time = funnel_pack_time(timestamp,j);
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

        this->data(state).update(
                datetime_viewer[row_num].to_unix_microsecond(),
                funnel_index_bitset[row_num],1);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        init_if_necessary(ctx, state);
        auto* input_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column));
        auto offset = input_column->offsets().get_data();
        auto elements = down_cast<Int64Column*>(ColumnHelper::get_data_column(input_column->elements_column().get()))->get_data();
        size_t begin = offset[row_num];
        size_t end = offset[row_num+1];
        this->data(state)._data.insert(this->data(state)._data.end(),elements.begin()+begin,elements.begin()+end);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }
        auto dst = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        const size_t size = this->data(state)._data.size();

        const auto int_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst->elements_column().get()));
        if (dst->elements_column()->is_nullable()){
            auto pNullableColumn = down_cast<NullableColumn*>(dst->elements_column().get());
            pNullableColumn->null_column()->get_data().insert(pNullableColumn->null_column()->get_data().end(),size,0);
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
        if (this->_init){
            return;
        }
        int constant_num = ctx->get_num_constant_columns();
        auto total_step = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(constant_num - 1));
        this->_total_step = total_step;
        this->_init = true;
    }

    void reset(){
        this->_data.clear();
        this->_init = false;
        this->_total_step = 0;
    }

    void update(const TimestampColumn* key_column,
                const Int64Column* value_column,
                const size_t start,
                const size_t end){
        for (size_t i = start; i < end; i++) {
            auto date_time = key_column->get_data()[i].to_unix_second();
            auto max_step = value_column->get_data()[i];
            auto column_size = this->_total_step;
            if (!this->_data.contains(date_time)) {
                this->_data[date_time] = std::vector<int64_t>(column_size, 0);
                fill_n(this->_data[date_time].begin(), max_step, 1);
                continue;
            }
            for (size_t k = 0; k<max_step;k++) {
                this->_data[date_time][k]++;
            }
        }
    }

    void merge(const Int64Column* elements,
        const size_t start,
        const size_t end){
        const auto& elements_data = elements->get_data();
        for (size_t i = start; i<end;i+=this->_total_step+1) {
            if (auto key = elements_data[i]; !this->_data.contains(key)) {
                std::vector<int64_t> value_vector = std::vector<int64_t>();
                value_vector.insert(
                    value_vector.begin(),
                    elements_data.begin() + i + 1,
                    elements_data.begin() + i + this->_total_step+1
                );
                this->_data[key] = value_vector;
            } else {
                // Update existing values
                std::vector<int64_t> value_vector = this->_data[key];
                for (size_t j = 0; j < this->_total_step; ++j) {
                    value_vector[j] += elements_data[i + j + 1];
                }
                this->_data[key] = value_vector;
            }
        }
    }
    phmap::flat_hash_map<int64_t,std::vector<int64_t>> _data = {};
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
        if((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()){
            return;
        }
        auto elements = down_cast<const MapColumn*>(ColumnHelper::get_data_column(columns[0]));
        auto map_size = elements->size();
        init_if_necessary(ctx, state,map_size);
        auto keys = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(elements->keys_column().get()));
        auto values = down_cast<const Int64Column*>(ColumnHelper::get_data_column(elements->values_column().get()));
        auto offset = elements->offsets();
        this->data(state).update(keys,values,offset.get_data()[row_num],offset.get_data()[row_num+1]);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column));
        // init_if_necessary(ctx, state,input_column->offsets().size()-1);
        auto offset = input_column->offsets();
        auto elements = down_cast<const Int64Column*>(ColumnHelper::get_data_column(input_column->elements_column().get()));
        auto start = offset.get_data()[row_num];
        auto end = offset.get_data()[row_num+1];
        if (!this->data(state)._init) {
            auto constant_num = ctx->get_num_constant_columns();
            auto total_step = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(constant_num - 1));
            this->data(state)._total_step =total_step;
            this->data(state)._init = true;
        }
        this->data(state).merge(elements,start,end);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()&& this->data(state)._data.size()==0) {
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(1);
            return;
        }
        if (to->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }
        auto dst_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        if (dst_column->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }
        if (dst_column->elements_column()->is_nullable()){
            auto dst = down_cast<NullableColumn*>(dst_column->elements_column().get());
            dst->null_column()->get_data().insert(dst->null_column()->get_data().end(),this->data(state)._data.size(),0);
        }
        auto data = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst_column->elements_column().get()));
        size_t element_size = 0;
        for (auto &[key,val]:this->data(state)._data) {
            data->append(key);
            element_size++;
            data->get_data().insert(data->get_data().end(),val.begin(),val.end());
            element_size+=this->data(state)._total_step;
        }
        auto offset = dst_column->offsets_column();
        offset->append(offset->get_data().back()+element_size);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()&& this->data(state)._data.size()==0) {
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(1);
            return;
        }
        if (to->is_nullable()){
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }

        auto map_column = down_cast<MapColumn*>(ColumnHelper::get_data_column(to));
        size_t group_size = this->data(state)._data.size();

        if (map_column->keys_column()->is_nullable()){
            auto dst = down_cast<NullableColumn*>(map_column->keys_column().get());
            dst->null_column()->get_data().insert(dst->null_column()->get_data().end(),group_size,0);

        }
        auto key_column = down_cast<TimestampColumn*>(ColumnHelper::get_data_column(map_column->keys_column().get()));
        // QUESTION: This value column should be ArrayColumn or StructColumn?
        if (map_column->values_column()->is_nullable()){
            auto dst = down_cast<NullableColumn*>(map_column->values_column().get());
            dst->null_column()->get_data().insert(dst->null_column()->get_data().end(),group_size,0);
        }

        auto value_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(map_column->values_column().get()));
        if (value_column->elements_column()->is_nullable()){
            auto dst = down_cast<NullableColumn*>(value_column->elements_column().get());
            dst->null_column()->get_data().insert(dst->null_column()->get_data().end(),group_size*this->data(state)._total_step,0);
        }

        auto value_elemens = down_cast<Int64Column*>(ColumnHelper::get_data_column(value_column->elements_column().get()));

        auto value_offset = value_column->offsets_column();

        auto map_offsets = map_column->offsets_column();
        // Init the offset
        value_elemens->get_data().reserve(group_size*this->data(state)._total_step);
        for (auto [key,val]:this->data(state)._data) {
            TimestampValue dst_key = TimestampValue::create_from_unixtime(key,cctz::utc_time_zone());
            dst_key.trunc_to_day();
            key_column->append(dst_key);
            value_elemens->get_data().insert(value_elemens->get_data().end(),val.begin(),val.end());
            value_offset->append(value_offset->get_data().back()+val.size());
        }
        map_offsets->append(map_offsets->get_data().back()+this->data(state)._data.size());
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
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

        auto element_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst_column->elements_column().get()))->get_data();

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