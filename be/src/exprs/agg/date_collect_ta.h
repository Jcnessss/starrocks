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
struct DateCollectState {

    void update(const DateColumn* column, size_t row_num) {
        DateValue value = column->get_data()[row_num];
        int date = numeric_date(value);
        int key = date / 100;
        int day = date % 100;
        if (!month_to_index.contains(key)) {
            uint64_t v = (static_cast<uint64_t>(key) << 32) | bool_values[day - 1];
            dates.emplace_back(v);
            month_to_index[key] = dates.size() - 1;
        } else {
            int index = month_to_index[key];
            dates[index] |= bool_values[day - 1];
        }
    }

    void merge(uint64_t date) {
        int key = date >> 32;
        if (!month_to_index.contains(key)) {
            dates.emplace_back(date);
            month_to_index[key] = dates.size() - 1;
        } else {
            int index = month_to_index[key];
            dates[index] = dates[index] |= date;
        }
    }

    int numeric_date(DateValue v) {
        int y, m, d;
        v.to_date(&y, &m, &d);
        return y * 10000 + 100 * m + d;
    }

    // Mask is used to identify top 50 bits.
    static inline uint64_t bool_values[] = {1UL << 31, 1UL << 30, 1UL << 29,
                                            1UL << 28, 1UL << 27, 1UL << 26, 1UL << 25, 1UL << 24, 1UL << 23, 1UL << 22,
                                            1UL << 21, 1UL << 20, 1UL << 19, 1UL << 18, 1UL << 17, 1UL << 16, 1UL << 15,
                                            1UL << 14, 1UL << 13, 1UL << 12, 1UL << 11, 1UL << 10, 1UL << 9, 1UL << 8,
                                            1UL << 7, 1UL << 6, 1UL << 5, 1UL << 4, 1UL << 3, 1UL << 2, 1UL << 1};
    bool _is_init = false;
    std::vector<uint64_t> dates;
    std::unordered_map<int, int> month_to_index;
};

class DateCollectFunction final
        : public AggregateFunctionBatchHelper<DateCollectState, DateCollectFunction> {
public:


    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()) {
            return;
        }
        auto column = down_cast<const DateColumn*>(ColumnHelper::get_data_column(columns[0]));
        this->data(state).update(column, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(column));
        Slice serialized = input_column->get(row_num).get_slice();
        size_t offset = 0;
        size_t size;
        std::memcpy(&size, serialized.data + offset, sizeof(size_t));
        offset += sizeof(size_t);
        for (int i = 0; i < size; i++) {
            uint64_t date;
            std::memcpy(&date, serialized.data + offset, sizeof(uint64_t));
            this->data(state).merge(date);
            offset += sizeof(uint64_t);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        serialize_state(this->data(state), down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to)));
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        std::vector<uint64_t> copy(this->data(state).dates);
        std::sort(copy.begin(), copy.end());
        serialize_state(this->data(state), down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to)));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        dst_column->reserve(chunk_size);

        const auto* src_column = down_cast<const DateColumn*>(src[0].get());
        for (size_t i = 0; i < chunk_size; ++i) {
            DateCollectState state;
            state.update(src_column, i);
            serialize_state(state, dst_column);
        }
    }

    void serialize_state(const DateCollectState& state, BinaryColumn* dst) const {
        Bytes& bytes = dst->get_bytes();
        size_t new_byte_size = bytes.size();
        size_t offset = bytes.size();
        new_byte_size += sizeof(size_t);
        for (int i = 0; i < state.dates.size(); i++) {
            new_byte_size += sizeof(uint64_t);
        }
        bytes.resize(new_byte_size);
        size_t size = state.dates.size();
        std::memcpy(bytes.data() + offset, &size, sizeof(size_t));
        offset += sizeof(size_t);
        for (uint64_t date : state.dates) {
          std::memcpy(bytes.data() + offset, &date, sizeof(uint64_t));
          offset += sizeof(uint64_t);
        }
        dst->get_offset().emplace_back(offset);
    }

    std::string get_name() const override { return "ta_date_collect"; }
};

} // namespace starrocks
