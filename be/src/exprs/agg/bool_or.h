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

#include "aggregate.h"
#include "column/column_helper.h"
namespace starrocks {
struct BooleanOrState {

    void update(const BooleanColumn* column, size_t row_num) {
        res |= column->get(row_num).get_uint8();
    }

    bool res = false;
};

class BooleanOrAggregateFunction final
        : public AggregateFunctionBatchHelper<BooleanOrState, BooleanOrAggregateFunction> {
public:

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()) {
            return;
        }
        auto column = down_cast<const BooleanColumn*>(ColumnHelper::get_data_column(columns[0]));
        this->data(state).update(column, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const BooleanColumn*>(ColumnHelper::get_data_column(column));
        this->data(state).res |= input_column->get(row_num).get_uint8();
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* dst_column = down_cast<BooleanColumn*>(ColumnHelper::get_data_column(to));
        dst_column->append(this->data(state).res);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    std::string get_name() const override { return "bool_or"; }
};

}
