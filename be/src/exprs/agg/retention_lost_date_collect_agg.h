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
struct RetentionLostState {

    void init(int time_unit_num, std::string time_unit) {
        _time_unit_num = time_unit_num;
        _time_unit = std::move(time_unit);
        _is_init = true;
        if (_time_unit == "day") {
            max_month_diff = time_unit_num / 28 + 1;
        } else if (_time_unit == "week") {
            max_month_diff = time_unit_num / 4 + 1;
        } else if (_time_unit == "month") {
            max_month_diff = time_unit_num;
        }
    }

    void update(const BinaryColumn* init_column, const BinaryColumn* return_column, size_t row_num) {
        LOG(INFO) << "ee";
        Slice init = init_column->get(row_num).get_slice();
        Slice return_date = return_column->get(row_num).get_slice();
        if (init.size == 0) {
            return ;
        }
        std::map<int, std::vector<DateValue>> init_dates;
        parse_init_dates(init, &init_dates);
        std::unordered_map<int, bool> is_found;
        if (init_dates.size() == 0 || return_date.size == 0) {
            return ;
        }
        is_found.reserve(init_dates.size());
        size_t offset = 0;
        size_t size;
        std::memcpy(&size, return_date.data + offset, sizeof(size_t));
        if (size == 0) {
            return;
        }
        offset += sizeof(size_t);
        auto it = init_dates.begin();
        for (int i = 0; i < size; i++) {
            uint64_t date;
            std::memcpy(&date, return_date.data + offset, sizeof(uint64_t));
            offset += sizeof(uint64_t);
            int year_month = date >> 32;
            int year = year_month / 100;
            int month = year_month % 100;
            uint32_t bitmap = date & split;
            if (year_month < it->first) {
                continue ;
            }
            while (it != init_dates.end() && year_month > it->first + max_month_diff) {
                it++;
            }
            if (it == init_dates.end()) {
                break;
            }
            DateValue pre_date = DateValue::MIN_DATE_VALUE;
            while (bitmap > 0) {
                int day = __builtin_clz(bitmap) + 1;
                DateValue v;
                v.from_date(year, month, day);
                auto tmp_it = it;
                while (tmp_it != init_dates.end()) {
                    if (tmp_it->first > year_month) {
                        break;
                    }
                    auto& dates = tmp_it->second;
                    for (const auto& init_date : dates) {
                        if (v < init_date) {
                            break ;
                        }
                        int diff = get_unit_diff(init_date, v);
                        int key = init_date.julian();
                        if (diff <= _time_unit_num) {
                            res[date_to_index[init_date.julian()]][diff + 2]++;
                            if (diff > 0 && pre_date <= init_date && !is_found.contains(key)) {
                                for (int j = 4 + _time_unit_num + diff; j < res[0].size(); j++) {
                                    res[date_to_index[init_date.julian()]][j]--;
                                }
                                is_found[key] = true;
                            }
                        }
                    }
                    tmp_it++;
                }
                pre_date = v;
                bitmap &= bool_values[day - 1];
            }
        }
    }

    void parse_init_dates(const Slice serialized, std::map<int, std::vector<DateValue>>* init_dates) {
        size_t offset = 0;
        size_t size;
        std::memcpy(&size, serialized.data + offset, sizeof(size_t));
        if (size == 0) {
            return;
        }
        offset += sizeof(size_t);
        for (int i = 0; i < size; i++) {
            uint64_t date;
            std::memcpy(&date, serialized.data + offset, sizeof(uint64_t));
            offset += sizeof(uint64_t);
            int year_month = date >> 32;
            int year = year_month / 100;
            int month = year_month % 100;
            uint64_t bitmap = date & split;
            std::vector<DateValue> month_dates;
            while (bitmap > 0) {
                int day = 32 - __builtin_ctzl(bitmap);
                DateValue v;
                v.from_date(year, month, day);
                month_dates.emplace_back(v);
                if (!date_to_index.contains(v.julian())) {
                    int index = 0;
                    for (; index < res.size(); index++) {
                        if (res[index][0] > v.julian()) {
                            break;
                        }
                    }
                    std::vector<int32_t> row;
                    row.resize(2 * _time_unit_num + 5);
                    row[0] = v.julian();
                    row[1] = 1;
                    for (int j = 3 + _time_unit_num; j < row.size(); j++) {
                        row[j] = 1;
                    }
                    res.insert(res.begin() + index, row);
                    push_back_map(index);
                    date_to_index[v.julian()] = index;
                } else {
                    res[date_to_index[v.julian()]][1]++;
                    for (int j = 3 + _time_unit_num; j < res[date_to_index[v.julian()]].size(); j++) {
                        res[date_to_index[v.julian()]][j]++;
                    }
                }
                bitmap = bitmap & (bitmap - 1);
            }
            std::reverse(month_dates.begin(), month_dates.end());
            (*init_dates)[year_month] = month_dates;
        }
    }

    void merge(std::vector<std::vector<int32_t>> other) {
        for (const auto& row : other) {
            int32_t date = row[0];
            if (!date_to_index.contains(date)) {
                int index = 0;
                for (; index < res.size(); index++) {
                    if (res[index][0] > date) {
                        break;
                    }
                }
                res.insert(res.begin() + index, row);
                push_back_map(index);
                date_to_index[date] = index;
            } else {
                int index = date_to_index[date];
                for (int i = 1; i < res[index].size(); i++) {
                    res[index][i] += row[i];
                }
            }
        }
    }

    void push_back_map(int index) {
        for (auto& entry : date_to_index) {
            if (entry.second >= index) {
                entry.second++;
            }
        }
    }

    int get_unit_diff(DateValue value, DateValue init_date) {
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

    void finalize_to_json_column(JsonColumn* json_column, cctz::time_zone time_zone) const {
        int64_t offset = 0;
        TimezoneUtils::timezone_offsets("UTC", time_zone.name() , &offset);
        vpack::Builder root;
        root.openObject();
        vpack::Builder retention;
        retention.openObject();
        for (const auto& row : res) {
            vpack::Builder builder_array;
            builder_array.openArray();
            for (int i = 1; i < _time_unit_num + 3; i++) {
                builder_array.add(vpack::Value(row[i]));
            }
            builder_array.close();
            DateValue v;
            v.set_julian(row[0]);
            TimestampValue timestamp_value;
            int y, m, d;
            v.to_date(&y, &m, &d);
            timestamp_value.from_timestamp(y, m, d, 0, 0, 0, 0);
            retention.add(std::to_string((timestamp_value.to_unix_second() - offset) * 1000), builder_array.slice());
        }
        retention.close();
        root.add("0", retention.slice());
        vpack::Builder lost;
        lost.openObject();
        for (const auto& row : res) {
            vpack::Builder builder_array;
            builder_array.openArray();
            for (int i = _time_unit_num + 3; i < row.size(); i++) {
                builder_array.add(vpack::Value(row[i]));
            }
            builder_array.close();
            DateValue v;
            v.set_julian(row[0]);
            TimestampValue timestamp_value;
            int y, m, d;
            v.to_date(&y, &m, &d);
            timestamp_value.from_timestamp(y, m, d, 0, 0, 0, 0);
            lost.add(std::to_string((timestamp_value.to_unix_second() - offset) * 1000), builder_array.slice());
        }
        lost.close();
        root.add("1", lost.slice());
        root.close();
        JsonValue json(root.slice());
        json_column->append(json);
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

    const static int64_t split = 0xFFFFFFFF;

    static inline uint64_t bool_values[] = {(1UL << 31) - 1, (1UL << 30) - 1, (1UL << 29) - 1, (1UL << 28) - 1, (1UL << 27) - 1,
                                            (1UL << 26) - 1, (1UL << 25) - 1, (1UL << 24) - 1, (1UL << 23) - 1, (1UL << 22) - 1,
                                            (1UL << 21) - 1, (1UL << 20) - 1, (1UL << 19) - 1, (1UL << 18) - 1, (1UL << 17) - 1,
                                            (1UL << 16) - 1, (1UL << 15) - 1, (1UL << 14) - 1, (1UL << 13) - 1, (1UL << 12) - 1,
                                            (1UL << 11) - 1, (1UL << 10) - 1, (1UL << 9) - 1, (1UL << 8) - 1, (1UL << 7) - 1,
                                            (1UL << 6) - 1, (1UL << 5) - 1, (1UL << 4) - 1, (1UL << 3) - 1, (1UL << 2) - 1, (1UL << 1) - 1};

    int _time_unit_num;
    std::string _time_unit;
    bool _is_init = false;
    int max_month_diff;
    std::vector<std::vector<int32_t>> res;
    std::unordered_map<int32_t, int> date_to_index;
};

class RetentionLostAggregateFunction final
        : public AggregateFunctionBatchHelper<RetentionLostState, RetentionLostAggregateFunction> {
public:

    void init_state_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state) const {
        if (this->data(state)._is_init) {
            return;
        }
        int constant_num = ctx->get_num_constant_columns();
        auto time_unit_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(constant_num - 2));
        auto time_unit = ColumnHelper::get_const_value<TYPE_VARCHAR>(ctx->get_constant_column(constant_num - 1));
        this->data(state).init(time_unit_num, time_unit.to_string());
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()) {
            return;
        }
        init_state_if_necessary(ctx, state);
        auto init_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[0]));
        auto return_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[1]));
        if (init_column->get(row_num).get_slice().size == 0) {
            return;
        }
        this->data(state).update(init_column, return_column, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        init_state_if_necessary(ctx, state);
        const auto* input_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(column));
        Slice serialized = input_column->get(row_num).get_slice();
        size_t offset = 0;
        size_t size;
        std::memcpy(&size, serialized.data + offset, sizeof(size_t));
        if (size == 0) {
            return;
        }
        offset += sizeof(size_t);
        std::vector<std::vector<int32_t>> other;
        other.resize(size);
        for (int i = 0; i < size; i++) {
            other[i].resize(2 * this->data(state)._time_unit_num + 5);
            for (int j = 0; j < 2 * this->data(state)._time_unit_num + 5; j++) {
                int32_t tmp;
                std::memcpy(&tmp, serialized.data + offset, sizeof(int32_t));
                other[i][j] = tmp;
                offset += sizeof(int32_t);
            }
        }
        this->data(state).merge(other);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        serialize_state(this->data(state), down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to)));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column()->append(0);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* json_column = down_cast<JsonColumn*>(ColumnHelper::get_data_column(to));
        this->data(state).finalize_to_json_column(json_column, ctx->state()->timezone_obj());
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(dst->get()));
        dst_column->reserve(chunk_size);

        int constant_num = ctx->get_num_constant_columns();
        auto time_unit_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(constant_num - 2));
        auto time_unit = ColumnHelper::get_const_value<TYPE_VARCHAR>(ctx->get_constant_column(constant_num - 1));

        const auto* init_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(src[0].get()));
        const auto* return_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(src[1].get()));
        for (size_t i = 0; i < chunk_size; ++i) {
            RetentionLostState state;
            state.init(time_unit_num, time_unit.to_string());
            state.update(init_column, return_column, i);
            serialize_state(state, dst_column);
            if (dst->get()->is_nullable()) {
                down_cast<NullableColumn*>(dst->get())->null_column()->append(0);
            }
        }
    }

    void serialize_state(const RetentionLostState& state, BinaryColumn* dst) const {
        Bytes& bytes = dst->get_bytes();
        const size_t old_size = bytes.size();
        int64_t total_size = 0;
        size_t offset = bytes.size();
        total_size += sizeof(size_t);
        total_size += sizeof(int32_t) * state.res.size() * state.res[0].size();
        const size_t new_size = old_size + total_size;
        bytes.resize(new_size);
        size_t size = state.res.size();
        std::memcpy(bytes.data() + offset, &size, sizeof(size_t));
        offset += sizeof(size_t);
        for (int i = 0; i < state.res.size(); i++) {
            for (int j = 0; j < state.res[0].size(); j++) {
                std::memcpy(bytes.data() + offset, &state.res[i][j], sizeof(int32_t));
                offset += sizeof(int32_t);
            }
        }
        dst->get_offset().emplace_back(new_size);
    }

    std::string get_name() const override { return "retention_lost_date_collect_agg"; }
};

} // namespace starrocks
