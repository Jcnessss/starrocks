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

#include "exprs/agg/attribution_data.h"

namespace starrocks {
struct AttributionRelationAggState : AttributionAggState {
    virtual ~AttributionRelationAggState() = default;

    void update(const Int64Column* id, const Int64Column* type, const TimestampColumn* time, const Int64Column* value,
        const DoubleColumn* _contribution, const ArrayColumn* source, const ArrayColumn* target,
        const BinaryColumn* source_prop,const ArrayColumn* target_prop, size_t row_num, MemPool* mem_pool) {

        AttributionEventEntity entity;

        entity.event_id = id->get_data()[row_num];
        entity.event_type = type->get_data()[row_num];
        entity.event_time = time->get_data()[row_num].to_unix_microsecond();
        entity.total_value = value->get_data()[row_num];
        entity.contribution = _contribution->get_data()[row_num];
        entities_size += entity.get_serialize_fix_size();

        entity.source_group = std::move(get_array_by_idx(source, row_num, mem_pool, entities_size));
        entity.target_group = std::move(get_array_by_idx(target, row_num, mem_pool, entities_size));
        entity.target_prop = std::move(get_array_by_idx(target_prop, row_num, mem_pool, entities_size));

        const Slice& slice = source_prop->get_slice(row_num);
        if (slice.size == 0) {
            entity.source_prop = {};
        } else {
            uint8_t* pos = mem_pool->allocate(slice.size);
            assert(pos != nullptr);
            std::memcpy(pos, slice.data, slice.size);
            entity.source_prop = Slice(pos, slice.size);
        }
        entities_size += sizeof(size_t);
        entities_size += slice.size;

        event_entity.emplace_back(entity);
    }

    void deserialize_slice(AttributionEventEntity& entity, char* data, size_t& offset, MemPool* mem_pool) {
        size_t cell_size;
        std::memcpy(&cell_size, data + offset, sizeof(size_t));
        offset += sizeof(size_t);

        uint8_t* pos = mem_pool->allocate(cell_size);
        assert(pos != nullptr);
        std::memcpy(pos, data + offset, cell_size);
        offset += cell_size;
        entity.source_prop = Slice(pos, cell_size);
    }

    void merge_impl(char* data, size_t& offset, size_t size, MemPool* mem_pool) override {
        while (offset < size) {
            AttributionEventEntity entity;
            deserialize_base_entity(entity, data, offset);
            deserialize_group(entity.source_group, data, offset, mem_pool);
            deserialize_group(entity.target_group, data, offset, mem_pool);
            deserialize_slice(entity, data, offset, mem_pool);
            deserialize_group(entity.target_prop, data, offset, mem_pool);
            entity.set_id_info();
            event_entity.emplace_back(std::move(entity));
        }
    }

    void serialize_slice(const Slice& slice, uint8_t* data, size_t& offset) const {
        std::memcpy(data + offset, &slice.size, sizeof(size_t));
        offset += sizeof(size_t);

        std::memcpy(data + offset, slice.data, slice.size);
        offset += slice.size;
    }

    void serialize_impl(uint8_t* data, size_t& offset) const override {
        for (const auto& ev : event_entity) {
            serialize_base_entity(ev, data, offset);
            serialize_group(ev.source_group, data, offset);
            serialize_group(ev.target_group, data, offset);
            serialize_slice(ev.source_prop, data, offset);
            serialize_group(ev.target_prop, data, offset);
        }
    }
};


class AttributionRelationAggFunction final
        : public AggregateFunctionBatchHelper<AttributionRelationAggState, AttributionRelationAggFunction> {

    public:

    void init_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state, bool merge = false) const {
        init_if_necessary(ctx, this->data(state), merge);
    }

    void init_if_necessary(FunctionContext* ctx, AttributionRelationAggState& state, bool merge = false) const {
        if (state.is_init) return;
        size_t const_num = ctx->get_num_constant_columns();
        size_t start = merge ? 1 : 5; // update/serialize const num is 10, merge is 4
        bool direct_in = down_cast<const BooleanColumn *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - start).get()))->get_data()[0];
        int64_t window_minute = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - start - 1).get()))->get_data()[0];
        int64_t attribution_type = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - start - 2).get()))->get_data()[0];

        state.init(direct_in, attribution_type, window_minute);
    }

    bool row_valid(const Columns& src, size_t row_num) const {
        constexpr static int column_num = 12;
        for (size_t col_num = 0; col_num < column_num; col_num++) {
            if ((src[col_num]->is_nullable() && src[col_num]->is_null(row_num)) || src[col_num]->only_null()) {
                return false;
            }
        }
        return true;
    }

    bool row_valid(const Column** src, size_t row_num) const {
        constexpr static int column_num = 12;
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
        const auto* source_prop = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[10]));
        const auto* target_prop = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[11]));

        this->data(state).update(id_column, type_column, time_column, value_column, contribution_column,
            source_column, target_column, source_prop, target_prop, row_num, ctx->mem_pool());
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
        const auto* source_prop = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(src[10].get()));
        const auto* target_prop = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(src[11].get()));

        for (size_t row_num = 0; row_num < chunk_size; row_num++) {
            if (row_valid(src, row_num)) {
                AttributionRelationAggState state;
                init_if_necessary(ctx, state);
                state.update(id_column, type_column, time_column, value_column, contribution_column, source_column,
                    target_column, source_prop, target_prop, row_num, ctx->mem_pool());
                state.serialize_to_intermediate(dst_column);
            } else {
                dst_column->get_offset().emplace_back(dst_column->get_offset().back());
            }
        }
        if (dst->get()->is_nullable()) {
            down_cast<NullableColumn*>(dst->get())->null_column_data().resize(dst->get()->size() + chunk_size);
        }
    }

    std::string get_name() const override {
        return "attribution_data_relation";
    }
};

} // namespace starrocks
