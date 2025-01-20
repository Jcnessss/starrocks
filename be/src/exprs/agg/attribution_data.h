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


#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "exprs/function_context.h"
#include "exprs/agg/attribution_helper.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "util/slice.h"


namespace starrocks {
struct AttributionAggState {
    bool is_init = false;
    bool direct_in;
    int64_t attribution_type;
    int64_t window_minute;
    int64_t entities_size = 0;
    mutable std::vector<AttributionEventEntity> event_entity;

    void init(bool _direct_in, int64_t _attribution_type, int64_t _window_minute) {
        if (is_init) return;
        direct_in = _direct_in;
        attribution_type = _attribution_type;
        window_minute = _window_minute;
        is_init = true;
    }

    std::vector<Slice> get_array_by_idx(const ArrayColumn* array, size_t row_num, MemPool* mem_pool, int64_t& entities_size) {
        const auto& [curr_offset, size] = array->get_element_offset_size(row_num);
        const auto& array_element = down_cast<const NullableColumn&>(array->elements());
        const auto* element_data_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(array->elements_column().get()));
        std::vector<Slice> curr;
        entities_size += sizeof(size_t); // slice 个数
        for (size_t i = curr_offset; i < curr_offset + size; i++) {
            entities_size += sizeof(size_t); // 记录每个slice的长度
            if (!array_element.is_null(i)) {
                const Slice& slice = element_data_column->get(i).get_slice();

                uint8_t* pos = mem_pool->allocate(slice.size);
                assert(pos != nullptr);
                std::memcpy(pos, slice.data, slice.size);
                curr.emplace_back(pos, slice.size);
                entities_size += slice.size; // slice 本身
            } else {
                curr.emplace_back();
            }
        }
        return curr;
    }

    void update(const Int64Column* id, const Int64Column* type, const TimestampColumn* time, const Int64Column* value,
        const DoubleColumn* _contribution, const ArrayColumn* source, const ArrayColumn* target, size_t row_num, MemPool* mem_pool) {
        AttributionEventEntity entity;

        entity.event_id = id->get_data()[row_num];
        entity.event_type = type->get_data()[row_num];
        entity.event_time = time->get_data()[row_num].to_unix_microsecond();
        entity.total_value = value->get_data()[row_num];
        entity.contribution = _contribution->get_data()[row_num];
        entities_size += entity.get_serialize_fix_size();
        entity.source_group = std::move(get_array_by_idx(source, row_num, mem_pool, entities_size));
        entity.target_group = std::move(get_array_by_idx(target, row_num, mem_pool, entities_size));

        event_entity.emplace_back(entity);
    }

    void deserialize_base_entity(AttributionEventEntity& entity, char* data, size_t& offset) {
        std::memcpy(&entity.event_id, data + offset, sizeof(int32_t));
        offset += sizeof(int32_t);

        std::memcpy(&entity.event_type, data + offset, sizeof(int32_t));
        offset += sizeof(int32_t);

        std::memcpy(&entity.event_time, data + offset, sizeof(int64_t));
        offset += sizeof(int64_t);

        std::memcpy(&entity.total_value, data + offset, sizeof(int64_t));
        offset += sizeof(int64_t);

        std::memcpy(&entity.contribution, data + offset, sizeof(double));
        offset += sizeof(double);
    }

    void deserialize_group(std::vector<Slice>& group, char* data, size_t& offset, MemPool* mem_pool) {
        size_t group_size;
        std::memcpy(&group_size, data + offset, sizeof(size_t));
        offset += sizeof(size_t);
        group.reserve(group_size);

        for (size_t j = 0; j < group_size; j++) {
            size_t cell_size;
            std::memcpy(&cell_size, data + offset, sizeof(size_t));
            offset += sizeof(size_t);

            uint8_t* pos = mem_pool->allocate(cell_size);
            assert(pos != nullptr);
            std::memcpy(pos, data + offset, cell_size);
            offset += cell_size;
            group.emplace_back(Slice(pos, cell_size));
        }
    }

    void merge(const BinaryColumn* intermediate, size_t row_num, MemPool* mem_pool) {
        Slice serialized = intermediate->get(row_num).get_slice();
        if (serialized.size == 0) {
            return;
        }

        size_t curr_offset = 0;
        size_t size;
        std::memcpy(&size, serialized.data + curr_offset, sizeof(size_t));
        curr_offset += sizeof(size_t);

        event_entity.reserve(event_entity.size() + size);

        merge_impl(serialized.data, curr_offset, serialized.size, mem_pool);
    }

    virtual void merge_impl(char* data, size_t& offset, size_t size, MemPool* mem_pool) {
        while (offset < size) {
            AttributionEventEntity entity;
            deserialize_base_entity(entity, data, offset);
            deserialize_group(entity.source_group, data, offset, mem_pool);
            deserialize_group(entity.target_group, data, offset, mem_pool);
            entity.set_id_info();
            event_entity.emplace_back(std::move(entity));
        }
    }

    void handle_struct_nullable(ArrayColumn* dst, size_t size) const {
        const auto& struct_column = down_cast<StructColumn*>(ColumnHelper::get_data_column(dst->elements_column().get()))->fields_column();

        if (dst->elements_column()->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(dst->elements_column().get());
            nullable_column->null_column()->get_data().resize(nullable_column->null_column()->get_data().size() + size);
        }
        // array里struct每个元素都不为null
        for (const auto& field : struct_column) {
            auto* nullable_column = down_cast<NullableColumn*>(field.get());
            nullable_column->null_column()->get_data().resize(nullable_column->null_column()->get_data().size() + size);
        }
    }

    void finalize_to_result(ArrayColumn* dst, bool relation_prop) const {
        AttributionHelper::fill_events_contribution(relation_prop, window_minute, attribution_type, event_entity);
        phmap::flat_hash_map<std::string, int64_t> event_count_map;
        phmap::flat_hash_map<BaseSourceEventGroup, std::vector<SourceEventEntity>, BaseSourceEventGroupHash, BaseSourceEventGroupEqual> event_array_map;
        AttributionHelper::handle_result(event_entity, event_count_map, event_array_map);

        size_t size = 0;

        const auto& struct_column = down_cast<StructColumn*>(ColumnHelper::get_data_column(dst->elements_column().get()))->fields_column();
        auto* id_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(struct_column[0].get()));
        auto* total_count_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(struct_column[1].get()));
        auto* valid_count_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(struct_column[2].get()));
        auto* contribution_column = down_cast<DoubleColumn*>(ColumnHelper::get_data_column(struct_column[3].get()));
        auto* source_group_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(ColumnHelper::get_data_column(struct_column[4].get())));
        auto* target_group_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(ColumnHelper::get_data_column(struct_column[5].get())));

        auto append_group = [](ArrayColumn* array_column, const std::vector<Slice>& group) {
            size_t group_array_offset = array_column->offsets().get_data().back();
            if (group.empty()) {
                if (array_column->elements_column()->is_nullable()) {
                    auto* nullable_column = down_cast<NullableColumn*>(array_column->elements_column().get());
                    nullable_column->null_column()->append(0);
                }
                array_column->offsets_column()->append(group_array_offset);
                return;
            }
            for (const auto& cell : group) {
                if (array_column->elements_column()->is_nullable()) {
                    auto* nullable_column = down_cast<NullableColumn*>(array_column->elements_column().get());
                    nullable_column->null_column()->append(0);
                }
                if (!cell.empty()) {
                    auto* element_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(array_column->elements_column().get()));
                    element_column->append(Slice(std::string(cell.data, cell.size)));
                    group_array_offset++;
                }
            }
            array_column->offsets_column()->append(group_array_offset);
        };

        for (const auto& [target_id_source, event_array] : event_array_map) {
            int64_t count = event_count_map[target_id_source.fetch_id_source()];
            // 聚合有效归因事件次数
            int64_t valid_count = event_array.size();
            // 聚合贡献值
            double contribution_sum = std::accumulate(event_array.begin(), event_array.end(), 0.0,
                [](double sum, const SourceEventEntity& e) {
                    return sum + e.contribution;
                });
            id_column->append(target_id_source.event_id);
            total_count_column->append(count);
            valid_count_column->append(valid_count);
            contribution_column->append(contribution_sum);

            append_group(source_group_column, target_id_source.source_group);
            append_group(target_group_column, target_id_source.target_group);

            size++;
        }

        if (direct_in) {
            std::vector<SourceEventEntity> target_array;
            AttributionHelper::fetch_target_array(target_array, event_entity);
            phmap::flat_hash_map<BaseSourceEventGroup, double, BaseSourceEventGroupHash, BaseSourceEventGroupEqual> direct_contribution_map;
            for (const auto& ev: target_array) {
                direct_contribution_map[ev.base_source_event_group] += ev.contribution;
            }
            for (const auto& [base_source_event_group, contribution]: direct_contribution_map) {
                id_column->append(base_source_event_group.event_id);
                total_count_column->append(0);
                valid_count_column->append(0);
                contribution_column->append(contribution);

                append_group(source_group_column, base_source_event_group.source_group);
                append_group(target_group_column, base_source_event_group.target_group);

                size++;
            }
        }

        handle_struct_nullable(dst, size);
        dst->offsets_column()->append(dst->offsets_column()->get_data().back() + size);
    }

    void serialize_base_entity(const AttributionEventEntity& ev, uint8_t* data, size_t& offset) const {
        std::memcpy(data + offset, &ev.event_id, sizeof(int32_t));
        offset += sizeof(int32_t);

        std::memcpy(data + offset, &ev.event_type, sizeof(int32_t));
        offset += sizeof(int32_t);

        std::memcpy(data + offset, &ev.event_time, sizeof(int64_t));
        offset += sizeof(int64_t);

        std::memcpy(data + offset, &ev.total_value, sizeof(int64_t));
        offset += sizeof(int64_t);

        std::memcpy(data + offset, &ev.contribution, sizeof(double));
        offset += sizeof(double);
    }

    void serialize_group(const std::vector<Slice>& group, uint8_t* data, size_t& offset) const {
        size_t size = group.size();
        std::memcpy(data + offset, &size, sizeof(size_t));
        offset += sizeof(size_t);

        for (const auto& slice : group) {
            std::memcpy(data + offset, &slice.size, sizeof(size_t));
            offset += sizeof(size_t);

            std::memcpy(data + offset, slice.data, slice.size);
            offset += slice.size;
        }
    }

    virtual void serialize_impl(uint8_t* data, size_t& offset) const {
        for (const auto& ev : event_entity) {
            serialize_base_entity(ev, data, offset);
            serialize_group(ev.source_group, data, offset);
            serialize_group(ev.target_group, data, offset);
        }
    }
    
    void serialize_to_intermediate(BinaryColumn* dst) const {
        Bytes& bytes = dst->get_bytes();
        size_t curr_offset = bytes.size();
        size_t new_byte_size = curr_offset + sizeof(size_t); // 行数

        new_byte_size += entities_size;
        bytes.resize(new_byte_size);

        size_t entities_num = event_entity.size();
        std::memcpy(bytes.data() + curr_offset, &entities_num, sizeof(size_t));
        curr_offset += sizeof(size_t);

        serialize_impl(bytes.data(), curr_offset);

        dst->get_offset().emplace_back(curr_offset);
    }
};


class AttributionAggFunction final
        : public AggregateFunctionBatchHelper<AttributionAggState, AttributionAggFunction> {
public:

    void init_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state, bool merge = false) const {
        init_if_necessary(ctx, this->data(state), merge);
    }

    void init_if_necessary(FunctionContext* ctx, AttributionAggState& state, bool merge = false) const {
        if (state.is_init) return;
        size_t const_num = ctx->get_num_constant_columns();
        size_t start = merge ? 1 : 3; // update/serialize const num is 10, merge is 4
        bool direct_in = down_cast<const BooleanColumn *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - start).get()))->get_data()[0];
        int64_t window_minute = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - start - 1).get()))->get_data()[0];
        int64_t attribution_type = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - start - 2).get()))->get_data()[0];

        state.init(direct_in, attribution_type, window_minute);
    }

    bool row_valid(const Columns& src, size_t row_num) const {
        constexpr static int column_num = 10;
        for (size_t col_num = 0; col_num < column_num; col_num++) {
            if ((src[col_num]->is_nullable() && src[col_num]->is_null(row_num)) || src[col_num]->only_null()) {
                return false;
            }
        }
        return true;
    }

    bool row_valid(const Column** src, size_t row_num) const {
        constexpr static int column_num = 10;
        for (size_t col_num = 0; col_num < column_num; col_num++) {
            if ((src[col_num]->is_nullable() && src[col_num]->is_null(row_num)) || src[col_num]->only_null()) {
                return false;
            }
        }
        return true;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if (!row_valid(columns, row_num)) {
            return;
        }
        init_if_necessary(ctx, state);

        const auto* id_column = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[0]));
        const auto* type_column = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[1]));
        const auto* time_column = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(columns[2]));
        const auto* contribution_column = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(columns[3]));
        const auto* value_column = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[4]));
        const auto* source_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[8]));
        const auto* target_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[9]));
        this->data(state).update(id_column, type_column, time_column, value_column, contribution_column, source_column, target_column, row_num, ctx->mem_pool());
    }


    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        if ((column->is_nullable() && column->is_null(row_num)) || column->only_null()) {
            return;
        }
        init_if_necessary(ctx, state, true);
        const auto* binary_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(column));
        this->data(state).merge(binary_column, row_num, ctx->mem_pool());
    }


    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        const auto& data = this->data(state).event_entity;
        auto* dst = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to));
        if (data.size() == 0) {
            dst->get_offset().emplace_back(dst->get_offset().back());
            if (to->is_nullable()) {
                down_cast<NullableColumn*>(to)->null_column()->append(0);
            }
            return;
        }
        this->data(state).serialize_to_intermediate(down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to)));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column()->append(0);
        }
    }


    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        this->data(state).finalize_to_result(down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to)), false);
        if (to->is_nullable()) {
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(dst->get()));
        const auto* id_column = down_cast<const Int64Column*>(ColumnHelper::get_data_column(src[0].get()));
        const auto* type_column = down_cast<const Int64Column*>(ColumnHelper::get_data_column(src[1].get()));
        const auto* time_column = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(src[2].get()));
        const auto* contribution_column = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(src[3].get()));
        const auto* value_column = down_cast<const Int64Column*>(ColumnHelper::get_data_column(src[4].get()));
        const auto* source_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(src[8].get()));
        const auto* target_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(src[9].get()));

        for (size_t row_num = 0; row_num < chunk_size; row_num++) {
            if (row_valid(src, row_num)) {
                AttributionAggState state;
                init_if_necessary(ctx, state);
                state.update(id_column, type_column, time_column, value_column, contribution_column, source_column, target_column, row_num, ctx->mem_pool());
                state.serialize_to_intermediate(dst_column);
            } else {
                dst_column->get_offset().emplace_back(dst_column->get_offset().back());
            }
        }
        if (dst->get()->is_nullable()) {
            down_cast<NullableColumn*>(dst->get())->null_column_data().resize(dst->get()->size() + chunk_size);
        }
    }

    std::string get_name() const override { return "attribution_data"; }
};

} // namespace starrocks