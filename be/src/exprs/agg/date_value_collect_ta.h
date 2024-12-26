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

#include <cstring>
#include <limits>
#include <type_traits>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gen_cpp/Data_types.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "thrift/protocol/TJSONProtocol.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks {
struct DateValueCollectState {

    void update(const DateColumn* date_column, const DoubleColumn* value_column, size_t row_num) {
        DateValue date = date_column->get_data()[row_num];
        dates.emplace_back(date.julian());
        double value = value_column->get_data()[row_num];
        values[date.julian()] = value;
    }

    void merge(int32_t date, double value) {
        dates.emplace_back(date);
        values[date] = value;
    }

    std::vector<int32_t> dates;
    phmap::flat_hash_map<int32_t, double> values;
};

class DateValueCollectFunction final
        : public AggregateFunctionBatchHelper<DateValueCollectState, DateValueCollectFunction> {
public:

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()) {
            return;
        }
        auto date_column = down_cast<const DateColumn*>(ColumnHelper::get_data_column(columns[0]));
        auto value_column = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(columns[1]));
        this->data(state).update(date_column, value_column, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        if ((column->is_nullable() && column->is_null(row_num)) || column->only_null()) {
            return;
        }
        const auto* input_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(column));
        Slice serialized = input_column->get(row_num).get_slice();
        size_t offset = 0;
        size_t size;
        std::memcpy(&size, serialized.data + offset, sizeof(size_t));
        offset += sizeof(size_t);
        for (int i = 0; i < size; i++) {
            int32_t date;
            std::memcpy(&date, serialized.data + offset, sizeof(int32_t));
            offset += sizeof(int32_t);
            double value;
            std::memcpy(&value, serialized.data + offset, sizeof(double));
            offset += sizeof(double);
            this->data(state).merge(date, value);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        serialize_state(this->data(state), down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to)));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column()->append(0);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        std::vector<int32_t> copy(this->data(state).dates);
        std::sort(copy.begin(), copy.end());
        auto* dst = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to));
        Bytes& bytes = dst->get_bytes();
        size_t offset = bytes.size();
        size_t size = copy.size();

        bytes.resize(bytes.size() + sizeof(size_t) + size * (sizeof(int32_t) + sizeof(double)));
        std::memcpy(bytes.data() + offset, &size, sizeof(size_t));

        offset += sizeof(size_t);
        for (int i = 0; i < copy.size(); i++) {
            std::memcpy(bytes.data() + offset, &copy[i], sizeof(int32_t));
            offset += sizeof(int32_t);
            std::memcpy(bytes.data() + offset, &this->data(state).values.at(copy[i]), sizeof(double));
            offset += sizeof(double);
        }
        dst->get_offset().emplace_back(offset);
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column()->append(0);
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column((*dst).get()));
        dst_column->reserve(chunk_size);

        const auto* date_column = down_cast<const DateColumn*>(ColumnHelper::get_data_column(src[0].get()));
        const auto* value_column = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(src[1].get()));
        Bytes& bytes = dst_column->get_bytes();
        size_t offset = bytes.size();
        bytes.resize(bytes.size() + chunk_size * (sizeof(size_t) + sizeof(int32_t) + sizeof(double)));
        for (size_t i = 0; i < chunk_size; ++i) {
            size_t size = 1;
            if ((src[0]->is_nullable() && src[0]->is_null(i)) || (src[1]->is_nullable() && src[1]->is_null(i))) {
                size = 0;
            }
            std::memcpy(bytes.data() + offset, &size, sizeof(size_t));
            offset += sizeof(size_t);
            if (size != 0) {
                std::memcpy(bytes.data() + offset, &date_column->get_data()[i], sizeof(int32_t));
                offset += sizeof(int32_t);
                std::memcpy(bytes.data() + offset, &value_column->get_data()[i], sizeof(double));
                offset += sizeof(double);
            }
            dst_column->get_offset().emplace_back(offset);
            if (dst->get()->is_nullable()) {
                down_cast<NullableColumn*>(dst->get())->null_column()->append(0);
            }
        }
    }

    void serialize_state(const DateValueCollectState& state, BinaryColumn* dst) const {
        Bytes& bytes = dst->get_bytes();
        size_t offset = bytes.size();
        size_t size = state.dates.size();

        bytes.resize(bytes.size() + sizeof(size_t) + size * (sizeof(int32_t) + sizeof(double)));
        std::memcpy(bytes.data() + offset, &size, sizeof(size_t));

        offset += sizeof(size_t);
        for (int i = 0; i < state.dates.size(); i++) {
            std::memcpy(bytes.data() + offset, &state.dates[i], sizeof(int32_t));
            offset += sizeof(int32_t);
            std::memcpy(bytes.data() + offset, &state.values.at(state.dates[i]), sizeof(double));
            offset += sizeof(double);
        }
        dst->get_offset().emplace_back(offset);
    }

    std::string get_name() const override { return "ta_date_value_collect"; }
};

} // namespace starrocks
