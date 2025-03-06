
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

#include <fmt/format.h>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/hash_set.h"
#include "column/map_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "util/phmap/phmap.h"
#include "util/time.h"

namespace starrocks {

struct EventEntity {
    int event_id;
    TimestampValue time;
    Slice attr;
    bool is_init;
};

struct UserSessionState {

    void init(int flag, int64_t interval) {
        time_interval_ms = interval;
        is_reverse = (flag & 0x10000) == 0x10000;
        max_length = flag & 0xffff;
        _is_init = true;
    }

    void update(MemPool* mem_pool, const Int32Column* event_column, const TimestampColumn* time_column,
                const BinaryColumn* attr_column, const Int8Column* flag_column, size_t row_num) {
        EventEntity entity;
        entity.event_id = event_column->get_data()[row_num];
        entity.time = time_column->get_data()[row_num];
        entity.is_init = flag_column->get_data()[row_num];
        Slice slice;
        if (attr_column->size() == 1) {
            slice = attr_column->get_data()[0];
        } else {
            slice = attr_column->get_data()[row_num];
        }
        if (slice.size == 0) {
            entity.attr = Slice();
        } else {
            uint8_t* pos = mem_pool->allocate(slice.size);
            assert(pos != nullptr);
            memcpy(pos, slice.data, slice.size);
            entity.attr = Slice(pos, slice.size);
        }
        data.emplace_back(entity);
        size += 17 + slice.size;
    }

    void merge(MemPool* mem_pool, Slice& serialized) {
        size_t offset = 0;
        while (offset < serialized.size) {
            EventEntity entity;
            std::memcpy(&entity.event_id, serialized.data + offset, sizeof(int));
            offset += sizeof(int);
            int64_t timestamp;
            std::memcpy(&timestamp, serialized.data + offset, sizeof(int64_t));
            TimestampValue v;
            v.set_timestamp(timestamp);
            entity.time = v;
            offset += sizeof(int64_t);
            std::memcpy(&entity.is_init, serialized.data + offset, sizeof(bool));
            offset += sizeof(bool);
            int length;
            std::memcpy(&length, serialized.data + offset, sizeof(int));
            offset += sizeof(int);
            uint8_t* pos = mem_pool->allocate(length);
            assert(pos != nullptr);
            std::memcpy(pos, serialized.data + offset, length);
            entity.attr = Slice(pos, length);
            data.emplace_back(entity);
            offset += length;
            size += 17 + length;
        }
    }

    void finalize_to_column(Column* to) const {
        std::vector<EventEntity> copy(data);

        auto* outer_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        auto* middle_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(outer_array->elements_column().get()));
        auto* inner_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(middle_array->elements_column().get()));

        if (is_reverse) {
            auto comparator_desc = [](const EventEntity& e1, const EventEntity& e2) { return e1.time > e2.time; };
            std::sort(copy.begin(), copy.end(), comparator_desc);
        } else {
            auto comparator_asc = [](const EventEntity& e1, const EventEntity& e2) { return e2.time > e1.time; };
            std::sort(copy.begin(), copy.end(), comparator_asc);
        }

        bool init_found = false;
        TimestampValue last_time;
        last_time.set_timestamp(0);
        int element_count = 0;
        int session_count = 0;
        for (const auto& entity : copy) {
            if (init_found) {
                if (time_diff(last_time, entity.time) <= time_interval_ms) {
                    append_element(inner_array, entity);
                    if (middle_array->elements_column()->is_nullable()) {
                        down_cast<NullableColumn*>(middle_array->elements_column().get())->null_column()->append(0);
                    }
                    last_time = entity.time;
                    element_count++;
                    if (element_count == max_length) {
                        init_found = false;
                        middle_array->offsets_column()->append(middle_array->offsets_column()->get_data().back() +
                                                               max_length);
                        if (outer_array->elements_column()->is_nullable()) {
                            down_cast<NullableColumn*>(outer_array->elements_column().get())->null_column()->append(0);
                        }
                        session_count++;
                    }
                    continue;
                } else {
                    init_found = false;
                    middle_array->offsets_column()->append(middle_array->offsets_column()->get_data().back() +
                                                           element_count);
                    element_count = 0;
                    if (outer_array->elements_column()->is_nullable()) {
                        down_cast<NullableColumn*>(outer_array->elements_column().get())->null_column()->append(0);
                    }
                    session_count++;
                }
            } else if (element_count == max_length && time_diff(last_time, entity.time) <= time_interval_ms) {
                last_time = entity.time;
                continue ;
            }
            if (entity.is_init) {
                append_element(inner_array, entity);
                if (middle_array->elements_column()->is_nullable()) {
                    down_cast<NullableColumn*>(middle_array->elements_column().get())->null_column()->append(0);
                }
                init_found = true;
                last_time = entity.time;
                element_count = 1;
            }
        }
        outer_array->offsets_column()->append(outer_array->offsets_column()->get_data().back() +
                                               session_count);
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column()->append(0);
        }
    }

    void append_element(ArrayColumn* inner_array, EventEntity entity) const {
        auto* element_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(inner_array->elements_column().get()));

        element_column->append(Slice(std::to_string(entity.event_id)));
        if (inner_array->elements_column()->is_nullable()) {
            down_cast<NullableColumn*>(inner_array->elements_column().get())->null_column()->append(0);
        }
        if (entity.attr.size == 0) {
            inner_array->offsets_column()->append(inner_array->offsets_column()->get_data().back() + 1);
        } else {
            element_column->append(entity.attr);
            if (inner_array->elements_column()->is_nullable()) {
                down_cast<NullableColumn*>(inner_array->elements_column().get())->null_column()->append(0);
            }
            inner_array->offsets_column()->append(inner_array->offsets_column()->get_data().back() + 2);
        }
    }

    int64_t time_diff(TimestampValue v1, TimestampValue v2) const {
        return std::abs(v2.to_unix_microsecond() / 1000 - v1.to_unix_microsecond() / 1000);
    }

    int64_t time_interval_ms;
    bool is_reverse;
    int max_length;
    bool _is_init = false;

    size_t size = 0;

    std::vector<EventEntity> data;
};

class UserSessionAggregateFunction final : public AggregateFunctionBatchHelper<UserSessionState, UserSessionAggregateFunction> {
public:

    void init_state_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state, bool is_update) const {
        if (this->data(state)._is_init) {
            return;
        }
        int constant_num = ctx->get_num_constant_columns();
        auto flag = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(constant_num - (is_update ? 3 : 2)));
        auto interval = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(constant_num - 1));
        this->data(state).init(flag, interval);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null() ||
            (columns[1]->is_nullable() && columns[1]->is_null(row_num)) || columns[1]->only_null() ||
            (columns[2]->is_nullable() && columns[2]->is_null(row_num)) || columns[2]->only_null()) {
            return;
        }
        init_state_if_necessary(ctx, state, true);
        auto* event_column = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[0]));
        auto* time_column = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(columns[1]));
        auto* attr_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[2]));
        auto* flag_column = down_cast<const Int8Column*>(ColumnHelper::get_data_column(columns[4]));
        this->data(state).update(ctx->mem_pool(), event_column, time_column, attr_column, flag_column, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        if ((column->is_nullable() && column->is_null(row_num)) || column->only_null()) {
            return;
        }
        init_state_if_necessary(ctx, state, false);
        const auto* input_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(column));
        Slice serialized = input_column->get_slice(row_num);
        if (serialized.size == 0) {
            return;
        }
        this->data(state).merge(ctx->mem_pool(), serialized);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        const auto& data = this->data(state).data;
        auto* dst = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to));
        if (data.size() == 0) {
            dst->get_offset().emplace_back(dst->get_offset().back());
            if (to->is_nullable()) {
                down_cast<NullableColumn*>(to)->null_column()->append(0);
            }
            return;
        }
        Bytes& bytes = dst->get_bytes();
        size_t offset = bytes.size();
        bytes.resize(dst->get_bytes().size() + this->data(state).size);
        for (const auto& entity : data) {
            std::memcpy(bytes.data() + offset, &entity.event_id, sizeof(int));
            offset += sizeof(int);
            std::memcpy(bytes.data() + offset, &entity.time._timestamp, sizeof(int64_t));
            offset += sizeof(int64_t);
            std::memcpy(bytes.data() + offset, &entity.is_init, sizeof(bool));
            offset += sizeof(bool);
            int length = entity.attr.size;
            std::memcpy(bytes.data() + offset, &length, sizeof(int));
            offset += sizeof(int);
            std::memcpy(bytes.data() + offset, entity.attr.data, entity.attr.size);
            offset += entity.attr.size;
        }
        dst->get_offset().emplace_back(offset);
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column()->append(0);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        this->data(state).finalize_to_column(to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(dst->get()));
        auto* event_column = down_cast<const Int32Column*>(ColumnHelper::get_data_column(src[0].get()));
        auto* time_column = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(src[1].get()));
        auto* attr_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(src[2].get()));
        auto* flag_column = down_cast<const Int8Column*>(ColumnHelper::get_data_column(src[4].get()));
        Bytes& bytes = column->get_bytes();
        size_t offset = bytes.size();
        size_t total_size = bytes.size();
        for (size_t i = 0; i < chunk_size; i++) {
            if ((src[0]->is_nullable() && src[0]->is_null(i)) || (src[1]->is_nullable() && src[1]->is_null(i)) ||
                (src[2]->is_nullable() && src[2]->is_null(i)))  {
                continue ;
            }
            if (attr_column->size() == 1) {
                total_size += 17 + attr_column->get_slice(0).size;
            } else {
                total_size += 17 + attr_column->get_slice(i).size;
            }
        }
        bytes.resize(total_size);
        for (size_t i = 0; i < chunk_size; i++) {
            if ((src[0]->is_nullable() && src[0]->is_null(i)) || (src[1]->is_nullable() && src[1]->is_null(i)) ||
                (src[2]->is_nullable() && src[2]->is_null(i)))  {
                column->get_offset().emplace_back(offset);
                continue ;
            }
            std::memcpy(bytes.data() + offset, &event_column->get_data()[i], sizeof(int));
            offset += sizeof(int);
            std::memcpy(bytes.data() + offset, &time_column->get_data()[i]._timestamp, sizeof(int64_t));
            offset += sizeof(int64_t);
            std::memcpy(bytes.data() + offset, &flag_column->get_data()[i], sizeof(bool));
            offset += sizeof(bool);
            Slice slice;
            if (attr_column->size() == 1) {
                slice = attr_column->get_slice(0);
            } else {
                slice = attr_column->get_slice(i);
            }
            int length = slice.size;
            std::memcpy(bytes.data() + offset, &length, sizeof(int));
            offset += sizeof(int);
            std::memcpy(bytes.data() + offset, slice.data, slice.size);
            offset += slice.size;
            column->get_offset().emplace_back(offset);
        }
        if (dst->get()->is_nullable()) {
            down_cast<NullableColumn*>(dst->get())->null_column_data().resize(dst->get()->size() + chunk_size);
        }
    }

    std::string get_name() const override { return "build_user_event_session_agg2"; }
};

} // namespace starrocks