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
#include "util/exprtk.hpp"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks {

struct RetentionLostValueFormulaQuotaState {

    void init(int time_unit_num, std::string time_unit, std::vector<int> compute_type, std::string formula_string,
              std::vector<std::string> quota_types, std::vector<int> days) {
        _time_unit_num = time_unit_num;
        _time_unit = std::move(time_unit);
        _quota_types = std::move(quota_types);
        _days = std::move(days);
        _compute_type = std::move(compute_type);
        formula = std::move(formula_string);
        _is_init = true;
    }

    void update(const BinaryColumn* init_column, const BinaryColumn* return_column, const BinaryColumn* value_column,
                size_t row_num) {
        if (value_num == 0) {
            value_num = _compute_type.size();
        }
        phmap::flat_hash_map<int32_t, std::vector<double>> date_to_value;
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
                            if (date_to_value.empty()) {
                                deserialize_values(value_column->get(row_num).get_slice(), &date_to_value);
                            }
                            for (int k = 0; k < value_num; k++) {
                                if (_compute_type[k] > 2) {
                                    for (int j = diff; j < _time_unit_num + 1; j++) {
                                        if (date_to_value[v.julian()].size() != 0) {
                                            sum[date_to_index[init_date.julian()]][j + k * (1 + _time_unit_num)] +=
                                                    date_to_value[v.julian()][k];
                                        }
                                    }
                                } else {
                                    if (date_to_value[v.julian()].size() != 0) {
                                        sum[date_to_index[init_date.julian()]][diff + k * (1 + _time_unit_num)] +=
                                                date_to_value[v.julian()][k];
                                    }
                                }
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

    void deserialize_values(Slice serialized, phmap::flat_hash_map<int32_t, std::vector<double>>* date_to_value) {
        if (serialized.size == 0) {
            return;
        }
        size_t offset = 0;
        size_t size;
        std::memcpy(&size, serialized.data + offset, sizeof(size_t));
        if (size == 0) {
            return;
        }
        offset += sizeof(size_t);
        offset += sizeof(size_t);
        for (int i = 0; i < size; i++) {
            int32_t date;
            std::memcpy(&date, serialized.data + offset, sizeof(int32_t));
            offset += sizeof(int32_t);
            std::vector<double> value_array;
            for (int j = 0; j < value_num; j++) {
                double value;
                std::memcpy(&value, serialized.data + offset, sizeof(double));
                offset += sizeof(double);
                value_array.emplace_back(value);
            }
            date_to_value->emplace(date, value_array);
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
                    std::vector<double> sum_row;
                    sum_row.resize(value_num * (1 + _time_unit_num));
                    res.insert(res.begin() + index, row);
                    sum.insert(sum.begin() + index, sum_row);
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

    void merge(std::vector<std::vector<int32_t>> other, std::vector<std::vector<double>> other_sum) {
        for (int i = 0; i < other.size(); i++) {
            int32_t date = other[i][0];
            if (!date_to_index.contains(date)) {
                int index = 0;
                for (; index < res.size(); index++) {
                    if (res[index][0] > date) {
                        break;
                    }
                }
                res.insert(res.begin() + index, other[i]);
                sum.insert(sum.begin() + index, other_sum[i]);
                push_back_map(index);
                date_to_index[date] = index;
            } else {
                int index = date_to_index[date];
                for (int j = 1; j < res[index].size(); j++) {
                    res[index][j] += other[i][j];
                }
                for (int j = 0; j < sum[index].size(); j++) {
                    sum[index][j] += other_sum[i][j];
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

    void finalize_to_map_column(MapColumn* map_column) const {
        auto* time_column = down_cast<TimestampColumn*>(ColumnHelper::get_data_column(map_column->keys_column().get()));
        auto* array_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(map_column->values_column().get()));
        auto* element_column = down_cast<DoubleColumn*>(ColumnHelper::get_data_column(array_column->elements_column().get()));
        auto& map_offsets = map_column->offsets().get_data();

        typedef double T;
        typedef exprtk::symbol_table<T> symbol_table_t;
        typedef exprtk::expression<T>   expression_t;
        typedef exprtk::parser<T>       parser_t;

        std::vector<T> variables;
        variables.resize(value_num);
        symbol_table_t symbol_table;
        for (int i = 0; i < value_num; i++) {
            symbol_table.add_variable("a" + std::to_string(i), variables[i]);
        }
        symbol_table.add_constants();

        expression_t expression;
        expression.register_symbol_table(symbol_table);

        for (int i = 0; i < res.size(); i++) {
            DateValue v;
            v.set_julian(res[i][0]);
            TimestampValue timestamp_value;
            int y, m, d;
            v.to_date(&y, &m, &d);
            timestamp_value.from_timestamp(y, m, d, 0, 0, 0, 0);
            time_column->get_data().emplace_back(timestamp_value);
            if (map_column->keys_column()->is_nullable()) {
                down_cast<NullableColumn*>(map_column->keys_column().get())->null_column()->append(0);
            }
            for (int j = 0; j < _quota_types.size(); j++) {
                std::vector<double> compute_res;
                compute_res.reserve(sum[0].size());
                for (int k = 0; k < value_num; k++) {
                    if (_compute_type[k] == 1 || _compute_type[k] == 3) {
                        for (int q = 0; q < _time_unit_num + 1; q++) {
                            compute_res[q +  k * (1 + _time_unit_num)] = sum[i][q +  k * (1 + _time_unit_num)];
                        }
                    } else if (_compute_type[k] == 2) {
                        for (int q = 0; q < _time_unit_num + 1; q++) {
                            compute_res[q +  k * (1 + _time_unit_num)] = sum[i][q +  k * (1 + _time_unit_num)] / res[i][q + 2];
                        }
                    } else {
                        for (int q = 0; q < _time_unit_num + 1; q++) {
                            compute_res[q +  k * (1 + _time_unit_num)] = sum[i][q +  k * (1 + _time_unit_num)] / res[i][1];
                        }
                    }
                }

                if (_quota_types[j] == "RETENTION_NUM") {
                    element_column->get_data().emplace_back(res[i][2 + _days[j]]);
                } else if (_quota_types[j] == "RETENTION_RATE") {
                    element_column->get_data().emplace_back(res[i][0] != 0 ? res[i][2 + _days[j]] / res[i][0] : 0);
                } else if (_quota_types[j] == "LOST_NUM") {
                    element_column->get_data().emplace_back(res[i][3 + _days[j] + _time_unit_num]);
                } else if (_quota_types[j] == "LOST_RATE") {
                    element_column->get_data().emplace_back(res[i][0] != 0 ? res[i][3 + _days[j] + _time_unit_num] / res[i][0] : 0);
                } else if (_quota_types[j] == "SIM_STAT") {
                    for (int k = 0; k < value_num; k++) {
                        variables[k] = compute_res[_days[j] +  k * (1 + _time_unit_num)];
                    }
                    auto result = expression.value();
                    if (std::isinf(result) || std::isnan(result)) {
                        element_column->get_data().emplace_back(0);
                    } else {
                        element_column->get_data().emplace_back(result);
                    }
                } else {
                    element_column->get_data().emplace_back(0);
                }
                if (array_column->elements_column()->is_nullable()) {
                    down_cast<NullableColumn*>(array_column->elements_column().get())->null_column()->append(0);
                }
            }
            array_column->offsets_column()->append(array_column->offsets().get_data().back() + _quota_types.size());
            if (map_column->values_column()->is_nullable()) {
                down_cast<NullableColumn*>(map_column->values_column().get())->null_column()->append(0);
            }
        }
        map_column->offsets_column()->append(map_offsets.back() + res.size());
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

    const static int64_t split = 0xFFFFFFFF;

    static inline uint64_t bool_values[] = {(1UL << 31) - 1, (1UL << 30) - 1, (1UL << 29) - 1, (1UL << 28) - 1, (1UL << 27) - 1,
                                            (1UL << 26) - 1, (1UL << 25) - 1, (1UL << 24) - 1, (1UL << 23) - 1, (1UL << 22) - 1,
                                            (1UL << 21) - 1, (1UL << 20) - 1, (1UL << 19) - 1, (1UL << 18) - 1, (1UL << 17) - 1,
                                            (1UL << 16) - 1, (1UL << 15) - 1, (1UL << 14) - 1, (1UL << 13) - 1, (1UL << 12) - 1,
                                            (1UL << 11) - 1, (1UL << 10) - 1, (1UL << 9) - 1, (1UL << 8) - 1, (1UL << 7) - 1,
                                            (1UL << 6) - 1, (1UL << 5) - 1, (1UL << 4) - 1, (1UL << 3) - 1, (1UL << 2) - 1, (1UL << 1) - 1};

    size_t value_num = 0;
    int _time_unit_num;
    std::string _time_unit;
    std::vector<int> _compute_type;
    std::vector<std::string> _quota_types;
    std::vector<int> _days;
    bool _is_init = false;
    std::vector<std::vector<int32_t>> res;
    std::vector<std::vector<double>> sum;
    std::unordered_map<int32_t, int> date_to_index;
    std::string formula;
};

class RetentionLostValueFormulaQuotaAggregateFunction final
        : public AggregateFunctionBatchHelper<RetentionLostValueFormulaQuotaState, RetentionLostValueFormulaQuotaAggregateFunction> {
public:

    void init_state_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state) const {
        if (this->data(state)._is_init) {
            return;
        }
        int constant_num = ctx->get_num_constant_columns();
        auto time_unit_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(constant_num - 3));
        auto time_unit = ColumnHelper::get_const_value<TYPE_VARCHAR>(ctx->get_constant_column(constant_num - 2));
        auto formula_string = ColumnHelper::get_const_value<TYPE_VARCHAR>(ctx->get_constant_column(constant_num - 1));
        auto column = ctx->get_constant_column(constant_num - 6);
        auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column.get()));
        auto offsets = array_column->offsets();
        auto int_column = down_cast<const Int32Column*>(ColumnHelper::get_data_column(array_column->elements_column().get()));
        std::vector<int> compute_type;
        for (int i = 0 ; i < offsets.get_data()[1]; i++) {
            compute_type.emplace_back(int_column->get_data()[i]);
        }
        auto column2 = ctx->get_constant_column(constant_num - 5);
        auto* array_column2 = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column2.get()));
        auto offsets2 = array_column2->offsets();
        auto varchar_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(array_column2->elements_column().get()));
        std::vector<std::string> quota_types;
        for (int i = 0 ; i < offsets2.get_data()[1]; i++) {
            quota_types.emplace_back(varchar_column->get_data()[i].to_string());
        }
        auto column3 = ctx->get_constant_column(constant_num - 4);
        auto array_column3 = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column3.get()));
        auto int_column2 = down_cast<const Int32Column*>(ColumnHelper::get_data_column(array_column3->elements_column().get()));
        std::vector<int> days;
        for (int i = 0 ; i < int_column->size(); i++) {
            days.emplace_back(int_column2->get_data()[i]);
        }
        this->data(state).init(time_unit_num, time_unit.to_string(), compute_type, formula_string.to_string(), quota_types, days);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()) {
            phmap::flat_hash_map<int32_t, std::vector<double>> date_to_value;
            auto value_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[2]));
            this->data(state).deserialize_values(value_column->get(row_num).get_slice(), &date_to_value);
            return;
        }
        init_state_if_necessary(ctx, state);
        auto init_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[0]));
        auto return_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[1]));
        auto value_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[2]));
        if (init_column->get(row_num).get_slice().size == 0) {
            phmap::flat_hash_map<int32_t, std::vector<double>> date_to_value;
            this->data(state).deserialize_values(value_column->get(row_num).get_slice(), &date_to_value);
            return;
        }
        this->data(state).update(init_column, return_column, value_column, row_num);
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
        std::memcpy(&this->data(state).value_num, serialized.data + offset, sizeof(size_t));
        offset += sizeof(size_t);
        std::vector<std::vector<int32_t>> other;
        other.resize(size);
        std::vector<std::vector<double>> other_sum;
        other_sum.resize(size);
        for (int i = 0; i < size; i++) {
            other[i].resize(2 * this->data(state)._time_unit_num + 5);
            for (int j = 0; j < 2 * this->data(state)._time_unit_num + 5; j++) {
                int32_t tmp;
                std::memcpy(&tmp, serialized.data + offset, sizeof(int32_t));
                other[i][j] = tmp;
                offset += sizeof(int32_t);
            }
            other_sum[i].resize(this->data(state).value_num * (this->data(state)._time_unit_num + 1));
            for (int j = 0; j < this->data(state).value_num * (this->data(state)._time_unit_num + 1); j++) {
                double tmp;
                std::memcpy(&tmp, serialized.data + offset, sizeof(double));
                other_sum[i][j] = tmp;
                offset += sizeof(double);
            }
        }
        this->data(state).merge(other, other_sum);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        serialize_state(this->data(state), down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to)));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column()->append(0);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* map_column = down_cast<MapColumn*>(ColumnHelper::get_data_column(to));
        this->data(state).finalize_to_map_column(map_column);
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column()->append(0);
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(dst->get()));
        dst_column->reserve(chunk_size);

        int constant_num = ctx->get_num_constant_columns();
        auto time_unit_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(constant_num - 3));
        auto time_unit = ColumnHelper::get_const_value<TYPE_VARCHAR>(ctx->get_constant_column(constant_num - 2));
        auto formula_string = ColumnHelper::get_const_value<TYPE_VARCHAR>(ctx->get_constant_column(constant_num - 1));
        auto column = ctx->get_constant_column(constant_num - 6);
        auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column.get()));
        auto offsets = array_column->offsets();
        auto int_column = down_cast<const Int32Column*>(ColumnHelper::get_data_column(array_column->elements_column().get()));
        std::vector<int> compute_type;
        for (int i = 0 ; i < offsets.get_data()[1]; i++) {
            compute_type.emplace_back(int_column->get_data()[i]);
        }
        auto column2 = ctx->get_constant_column(constant_num - 5);
        auto* array_column2 = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column2.get()));
        auto offsets2 = array_column2->offsets();
        auto varchar_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(array_column2->elements_column().get()));
        std::vector<std::string> quota_types;
        for (int i = 0 ; i < offsets2.get_data()[1]; i++) {
            quota_types.emplace_back(varchar_column->get_data()[i].to_string());
        }
        auto column3 = ctx->get_constant_column(constant_num - 4);
        auto array_column3 = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column3.get()));
        auto int_column2 = down_cast<const Int32Column*>(ColumnHelper::get_data_column(array_column3->elements_column().get()));
        std::vector<int> days;
        for (int i = 0 ; i < int_column->size(); i++) {
            days.emplace_back(int_column2->get_data()[i]);
        }

        const auto* init_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(src[0].get()));
        const auto* return_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(src[1].get()));
        const auto* value_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(src[2].get()));
        for (size_t i = 0; i < chunk_size; ++i) {
            RetentionLostValueFormulaQuotaState state;
            state.init(time_unit_num, time_unit.to_string(), compute_type, formula_string.to_string(), quota_types, days);
            state.update(init_column, return_column, value_column, i);
            serialize_state(state, dst_column);
        }
    }

    void serialize_state(const RetentionLostValueFormulaQuotaState& state, BinaryColumn* dst) const {
        Bytes& bytes = dst->get_bytes();
        const size_t old_size = bytes.size();
        int64_t total_size = 0;
        size_t offset = bytes.size();
        total_size += 2 * sizeof(size_t);
        total_size += sizeof(int32_t) * state.res.size() * state.res[0].size() +
              state.sum.size() * state.sum[0].size() * sizeof(double);
        const size_t new_size = old_size + total_size;
        bytes.resize(new_size);
        size_t size = state.res.size();
        std::memcpy(bytes.data() + offset, &size, sizeof(size_t));
        offset += sizeof(size_t);
        std::memcpy(bytes.data() + offset, &state.value_num, sizeof(size_t));
        offset += sizeof(size_t);
        for (int i = 0; i < state.res.size(); i++) {
            for (int j = 0; j < state.res[0].size(); j++) {
                std::memcpy(bytes.data() + offset, &state.res[i][j], sizeof(int32_t));
                offset += sizeof(int32_t);
            }
            for (int j = 0; j < state.sum[0].size(); j++) {
                std::memcpy(bytes.data() + offset, &state.sum[i][j], sizeof(double));
                offset += sizeof(double);
            }
        }
        dst->get_offset().emplace_back(new_size);
    }

    std::string get_name() const override { return "retention_lost_date_sim_stat_formula_quota_array_agg"; }
};

} // namespace starrocks
