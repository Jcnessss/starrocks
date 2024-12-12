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

#include "column/map_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "exprs/ta_functions.h"
#include "gen_cpp/Data_types.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "util/slice.h"

namespace starrocks {

struct IntervalStateConfig {
    constexpr static int header_len = 16;
    constexpr static int event_item_len_non_self = 9;
    constexpr static int event_item_len_self = 8;
    constexpr static int event_type_start = 0;
    constexpr static int event_type_end = 1;
    constexpr static int interval_unit_sec = 0;
};

struct IntervalState {
    bool is_init = false;
    bool is_self_case;
    int32_t interval_unit;
    int64_t window_gaps;
    std::vector<int32_t> event_id;
    std::vector<uint64_t> event_time;

    void init(int32_t _interval_unit, int64_t _session_flag, int64_t _window_gaps) {
        if (is_init) return;
        is_self_case = ((_session_flag & 0x1) == 0x1);
        interval_unit = _interval_unit;
        window_gaps = _window_gaps;
        is_init = true;
    }

    void update(const Int32Column* id_column, const TimestampColumn* time_column , size_t row_num) {
        event_time.emplace_back(TaFunctions::microsToMillis(time_column->get_data()[row_num].to_unix_microsecond()));
        if (!is_self_case) {
            event_id.emplace_back(id_column->get_data()[row_num]);
        }
    }

    virtual void merge(const BinaryColumn* intermediate, size_t row_num) {
        const Slice& serialized = intermediate->get(row_num).get_slice();
        size_t curr_offset = 0;
        size_t size;
        std::memcpy(&size, serialized.data + curr_offset, sizeof(size_t));
        curr_offset += sizeof(size_t);
        uint64_t* time_array = reinterpret_cast<uint64_t*>(serialized.data + curr_offset);
        for (int i = 0; i < size; i++) {
            event_time.push_back(time_array[i]);
        }
        if (!is_self_case) {
            curr_offset += (sizeof(uint64_t) * size);
            int32_t * id_array = reinterpret_cast<int32_t*>(serialized.data + curr_offset);
            for (int i = 0; i < size; i++) {
                event_id.push_back(id_array[i]);
            }
        }
    }

    /**
     * MapColumn
     * @param dst
     */
    void finalize_to_result(MapColumn* dst) const {
        std::vector<size_t> indices(event_time.size());
        for (size_t i = 0; i < event_time.size(); i++) indices[i] = i;
        std::stable_sort(indices.begin(), indices.end(), [&] (size_t index1, size_t index2) {
            if (is_self_case) {
                return event_time[index1] < event_time[index2];
            } else {
                if (event_time[index1] < event_time[index2]) {
                    return true;
                } else if (event_time[index1] > event_time[index2]) {
                    return false;
                } else {
                    return event_id[index1] > event_id[index2];
                }
            }
        });
        auto* key_column = down_cast<TimestampColumn*>(ColumnHelper::get_data_column(dst->keys_column().get()));
        if (dst->keys_column()->is_nullable()) {
            auto nullableColumn = down_cast<NullableColumn*>(dst->keys_column().get());
            nullableColumn->null_column()->append(0);
        }

        auto* value_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst->values_column().get()));
        if (dst->values_column()->is_nullable()) {
            auto nullableColumn = down_cast<NullableColumn*>(dst->values_column().get());
            nullableColumn->null_column()->append(0);
        }

        size_t size = 0;
        if (is_self_case) {
            finalize_self_case(indices, key_column, value_column, size);
        } else {
            finalize_non_self_case(indices, key_column, value_column, size);
        }
        dst->offsets_column()->append(dst->offsets_column()->get_data().back() + size);
    }

    void finalize_to_dst(TimestampColumn* time_column,
                         Int64Column* interval_column,
                         size_t& size, int64_t time, int64_t interval) const {
        int64_t unix_time = TaFunctions::millisToMicros(time);
        int64_t second = unix_time / 1000000;
        int64_t micro = unix_time % 1000000;
        TimestampValue to_add;
        to_add.from_unixtime(second, micro, cctz::utc_time_zone());
        time_column->append(std::move(to_add));
        interval_column->append(get_output_interval(interval));
        size++;
    }

    void finalize_self_case(std::vector<size_t>& indices,
                            TimestampColumn* time_column,
                            Int64Column* interval_column,
                            size_t& size) const {
        int64_t start_time = -1;
        for (size_t index : indices) {
            if (start_time == -1) {
                start_time = event_time[index];
            } else {
                if (start_time > 0) {
                    int64_t interval = event_time[index] - start_time;
                    // AA case interval 不允许为0
                    if (interval > 0 && interval <= window_gaps) {
                        finalize_to_dst(time_column, interval_column, size, start_time, interval);
                    }
                    start_time = event_time[index];
                }
            }
        }
    }

    void finalize_non_self_case(std::vector<size_t>& indices,
                                TimestampColumn* time_column,
                                Int64Column* interval_column,
                                size_t& size) const {
        int64_t start_time = -1;
        for (size_t index : indices) {
            if (event_id[index] == IntervalStateConfig::event_type_start) {
                start_time = event_time[index];
            } else {
                int64_t interval = event_time[index] - start_time;
                // 如果为0，跳过该次B事件
                if (interval <= 0) {
                    continue;
                } else if (interval <= window_gaps) {
                    if (start_time > 0) {
                        finalize_to_dst(time_column, interval_column, size, start_time, interval);
                    }
                }
                start_time = -1;
            }
        }
    }

    /**
     * BinaryColumn
     * @param dst
     */
    virtual void serialize_to_intermediate(BinaryColumn* dst) const {
        Bytes& bytes = dst->get_bytes();
        size_t curr_offset = bytes.size();
        size_t new_byte_size = curr_offset + sizeof(size_t); // 行数

        new_byte_size += sizeof(uint64_t) * event_time.size(); // event_time size
        if (!is_self_case) {
            new_byte_size += sizeof(int32_t) * event_id.size(); // event_id size
        }

        bytes.resize(new_byte_size);
        size_t size = event_time.size();
        std::memcpy(bytes.data() + curr_offset, &size, sizeof(size_t));
        curr_offset += sizeof(size_t);

        // Serialize event_time
        std::memcpy(bytes.data() + curr_offset, event_time.data(), sizeof(uint64_t) * event_time.size());
        curr_offset += sizeof(uint64_t) * event_time.size();

        // Serialize event_id if not self case
        if (!is_self_case) {
            std::memcpy(bytes.data() + curr_offset, event_id.data(), sizeof(int32_t) * event_id.size());
            curr_offset += sizeof(int32_t) * event_id.size();
        }

        dst->get_offset().push_back(curr_offset);
    }

    int32_t get_output_interval(int64_t interval) const {
        if (interval_unit == IntervalStateConfig::interval_unit_sec) {
            interval = TaFunctions::millsToSecsCeil(interval);
        }
        return interval;
    }
};

class IntervalAggFunction final
        : public AggregateFunctionBatchHelper<IntervalState, IntervalAggFunction> {

public:

    void init_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state) const {
        init_if_necessary(ctx, this->data(state));
    }

    void init_if_necessary(FunctionContext* ctx, IntervalState& state) const {
        if (state.is_init) {
            return;
        }
        int64_t flag = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(2).get()))->get_data()[0];
        int64_t window_gaps = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(3).get()))->get_data()[0];
        int64_t interval_unit = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(4).get()))->get_data()[0];
        state.init(interval_unit, flag, window_gaps);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        // Key could not be null.
        if ((columns[1]->is_nullable() && columns[1]->is_null(row_num)) || columns[1]->only_null()) {
            return;
        }
        init_if_necessary(ctx, state);

        const auto* id_column = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[0]));
        const auto* time_column = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(columns[1]));

        this->data(state).update(id_column, time_column, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        init_if_necessary(ctx, state);
        const auto* binary_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(column));
        this->data(state).merge(binary_column, row_num);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        this->data(state).serialize_to_intermediate(down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to)));
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()) {
            auto null_column = down_cast<NullableColumn*>(to)->null_column();
            null_column->append(0);
        }
        this->data(state).finalize_to_result(down_cast<MapColumn*>(ColumnHelper::get_data_column(to)));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {

        auto* dst_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(dst->get()));
        dst_column->reserve(chunk_size);
        const auto* id_column = down_cast<const Int32Column*>(ColumnHelper::get_data_column(src[0].get()));
        const auto* time_column = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(src[1].get()));

        for (size_t i = 0; i < chunk_size; i++) {
            IntervalState state;
            init_if_necessary(ctx, state);
            state.update(id_column, time_column, i);
            state.serialize_to_intermediate(dst_column);
        }
    }

    std::string get_name() const override { return "build_user_interval_agg"; }
};


} // namespace starrocks