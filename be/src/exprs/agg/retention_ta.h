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
struct RetentionTaState {

    void init(int time_unit_num, std::string time_unit) {
        _time_unit_num = time_unit_num;
        _time_unit = std::move(time_unit);
        _init_date = DateValue::MAX_DATE_VALUE;
        _is_init = true;
    }

    void update(const DateColumn* column, size_t row_num) {
        DateValue value = column->get_data()[row_num];
        LOG(INFO) << value.to_string();
        if (value < _init_date) {
            move_bit_if_necessary(value);
        } else if (value > _init_date) {
            int offset = get_unit_diff(value);
            LOG(INFO) << offset;
            _bitmap |= bool_values[offset];
        } else {
            _bitmap |= bool_values[0];
        }
    }

    int get_unit_diff(DateValue value) {
        if (_init_date == DateValue::MAX_DATE_VALUE) {
            return 0;
        }
        int diff = 0;
        int init_year = year(_init_date);
        int value_year = year(value);
        if (_time_unit == "day") {
            diff = value.julian() - _init_date.julian();
        } else if (_time_unit == "week") {
            diff = week(value) - week(_init_date);
        } else if (_time_unit == "month") {
            diff = month(value) - month(_init_date) + (value_year - init_year) * 12;
        }
        return std::abs(diff);
    }

    void move_bit_if_necessary(DateValue value) {
        if (_init_date != DateValue::MAX_DATE_VALUE) {
            int move_bits = get_unit_diff(value);
            LOG(INFO) << move_bits;
            _bitmap = _bitmap >> move_bits;
        }
        _init_date = value;
        _bitmap |= bool_values[0];
    }

    void finalize_to_array_column(ArrayColumn* array_column) const {
        DatumArray array;
        LOG(INFO) << _time_unit_num;
        array.reserve(_time_unit_num);
        int i = 0;
        for (; i < _time_unit_num; ++i) {
            if ((_bitmap & bool_values[i]) == 0) {
                break;
            }
            array.emplace_back((uint8_t)((_bitmap & bool_values[i]) > 0));
        }
        for (; i < _time_unit_num; ++i) {
            array.emplace_back((uint8_t)0);
        }
        array_column->append_datum(array);
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

    uint64_t _bitmap;

    // Mask is used to identify top 50 bits.
    static inline uint64_t bool_values[] = {1UL << 63, 1UL << 62, 1UL << 61, 1UL << 60, 1UL << 59, 1UL << 58, 1UL << 57,
                                            1UL << 56, 1UL << 55, 1UL << 54, 1UL << 53, 1UL << 52, 1UL << 51, 1UL << 50,
                                            1UL << 49, 1UL << 48, 1UL << 47, 1UL << 46, 1UL << 45, 1UL << 44, 1UL << 43,
                                            1UL << 42, 1UL << 41, 1UL << 40, 1UL << 39, 1UL << 38, 1UL << 37, 1UL << 36,
                                            1UL << 35, 1UL << 34, 1UL << 33, 1UL << 32, 1UL << 31, 1UL << 30, 1UL << 29,
                                            1UL << 28, 1UL << 27, 1UL << 26, 1UL << 25, 1UL << 24, 1UL << 23, 1UL << 22,
                                            1UL << 21, 1UL << 20, 1UL << 19, 1UL << 18, 1UL << 17, 1UL << 16, 1UL << 15,
                                            1UL << 14, 1UL << 13};
    int _time_unit_num;
    std::string _time_unit;
    bool _is_init = false;
    DateValue _init_date;
};

class RetentionTaAggregateFunction final
        : public AggregateFunctionBatchHelper<RetentionTaState, RetentionTaAggregateFunction> {
public:

    void init_state_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state) const {
        if (this->data(state)._is_init) {
            return;
        }
        auto time_unit_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
        LOG(INFO) << time_unit_num;
        auto time_unit = ColumnHelper::get_const_value<TYPE_VARCHAR>(ctx->get_constant_column(2));
        LOG(INFO) << time_unit;
        this->data(state).init(time_unit_num, time_unit.to_string());
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()) {
            return;
        }
        init_state_if_necessary(ctx, state);
        auto column = down_cast<const DateColumn*>(ColumnHelper::get_data_column(columns[0]));
        this->data(state).update(column, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        init_state_if_necessary(ctx, state);
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        JulianDate other_julian = static_cast<JulianDate>(input_column->get(row_num).get<DatumArray>().data()[0].get_int64());
        uint64_t other_bitmap = static_cast<uint64_t>(input_column->get(row_num).get<DatumArray>().data()[1].get_int64());
        LOG(INFO) << other_bitmap;
        DateValue other_init_date;
        other_init_date.set_julian(other_julian);
        LOG(INFO) << other_init_date.to_string();
        if (this->data(state)._init_date > other_init_date) {
            this->data(state).move_bit_if_necessary(other_init_date);
        } else if (this->data(state)._init_date < other_init_date) {
            int date_diff = this->data(state).get_unit_diff(other_init_date);
            LOG(INFO) << date_diff;
            other_bitmap = other_bitmap >> date_diff;
        }
        this->data(state)._bitmap |= other_bitmap;
        LOG(INFO) << this->data(state)._bitmap;
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DatumArray array;
        array.emplace_back(static_cast<int64_t>(this->data(state)._init_date.julian()));
        array.emplace_back(this->data(state)._bitmap);
        down_cast<ArrayColumn*>(to)->append_datum(array);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        this->data(state).finalize_to_array_column(array_column);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<ArrayColumn*>((*dst).get());
        dst_column->reserve(chunk_size);

        const auto* src_column = down_cast<const DateColumn*>(src[0].get());
        for (size_t i = 0; i < chunk_size; ++i) {
            RetentionTaState state;
            state.update(src_column, i);
            DatumArray array;
            array.emplace_back(state._init_date.julian());
            array.emplace_back(state._bitmap);
            dst_column->append_datum(array);
        }
    }

    std::string get_name() const override { return "retention_ta"; }
};

} // namespace starrocks
