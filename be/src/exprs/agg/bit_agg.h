//
// Created by Co1a on 24-11-15.
//
#pragma once

#include "column/column_helper.h"

namespace starrocks {

struct BitwishAggState {
    void reset(){
        this->result= 0;
    }
    void update(int64_t t){
        if (!_init){
            this->result = t;
            _init = true;
        }
        this->result |= t;
    }
    int64_t result = 0;
    bool _init = false;
};

class BitwishAggregateFunction final : public AggregateFunctionBatchHelper<BitwishAggState, BitwishAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state)const override{
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()) {
            return;
        }
        auto elements = down_cast<const Int64Column*>(columns[0]);
        this->data(state).update(elements->get_data()[row_num]);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        auto input_column = down_cast<const Int64Column*>(column);
        if (input_column->is_null(row_num)) {
            return;
        }
        this->data(state).result |= input_column->get_data()[row_num];
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto dst = down_cast<Int64Column*>(to);
        dst->append_datum(this->data(state).result);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* int_column = down_cast<Int64Column*>(to);
        int_column->append(this->data(state).result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,ColumnPtr* dst) const override {
        auto dst_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst->get()));
        auto elements = down_cast<Int64Column*>(ColumnHelper::get_data_column((src[0].get())));
        for (size_t i = 0; i < chunk_size; ++i) {
            if (elements->is_null(i)) {
                continue;
            }
            int64_t value = elements->get_data()[i];
            dst_column->append(value);
        }
    }

    std::string get_name() const override { return "bitwise_or_agg"; }
};

} // namespace starrocks