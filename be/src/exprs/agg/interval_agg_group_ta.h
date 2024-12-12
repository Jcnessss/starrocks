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

#include "exprs/agg/interval_agg_ta.h"
#include "column/array_column.h"
#include "column/struct_column.h"
#include "column/column_builder.h"

namespace starrocks {
struct IntervalGroupState : IntervalState {
virtual ~IntervalGroupState() = default;
    bool group_const;
    int64_t groups_num;
    std::vector<std::vector<std::string>> groups;

    void init(int32_t _interval_unit, int64_t _session_flag, int64_t _window_gaps, int64_t _group_nums, bool _group_const, std::vector<std::string>& group) {
        if (is_init) return;
        groups_num = _group_nums;
        group_const = _group_const;
        if (group_const) {
            groups.emplace_back(std::move(group));
        }
        IntervalState::init(_interval_unit, _session_flag, _window_gaps);
    }

    void update(const Int32Column* id_column, const TimestampColumn* time_column, const ArrayColumn* group_column, size_t row_num) {
        IntervalState::update(id_column, time_column, row_num);
        if (group_const) {
            return;
        }
        const auto& [curr_offset, size] = group_column->get_element_offset_size(row_num);
        int32_t _event_id = id_column->get_data()[row_num];
        if (_event_id == IntervalStateConfig::event_type_start && size > 0) {
            const auto& array_element = down_cast<const NullableColumn&>(group_column->elements());
            const auto* element_data_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(group_column->elements_column().get()));
            std::vector<std::string> curr;
            for (size_t i = curr_offset; i < curr_offset + size; i++) {
                if (!array_element.is_null(i)) {
                    const Slice& slice = element_data_column->get(i).get_slice();
                    std::string group(slice.data, slice.size);
                    curr.emplace_back(std::move(group));
                } else {
                    curr.emplace_back();
                }
            }
            groups.emplace_back(curr);
        } else {
            groups.emplace_back();
        }
    }

    void merge(const BinaryColumn* intermediate, size_t row_num) override {
        const Slice& serialized = intermediate->get(row_num).get_slice();
        size_t curr_offset = 0;
        size_t size;
        std::memcpy(&size, serialized.data + curr_offset, sizeof(size_t)); // 行数
        curr_offset += sizeof(size_t);
        uint64_t* time_array = reinterpret_cast<uint64_t*>(serialized.data + curr_offset);
        for (int i = 0; i < size; i++) {
            event_time.emplace_back(time_array[i]);
        }
        curr_offset += (sizeof(uint64_t) * size);
        if (!is_self_case) {
            int32_t * id_array = reinterpret_cast<int32_t*>(serialized.data + curr_offset);
            for (int i = 0; i < size; i++) {
                event_id.emplace_back(id_array[i]);
            }
            curr_offset += (sizeof(int32_t) * size);
        }
        if (group_const) size = 1; // group vector size
        for (int i = 0; i < size; i++) {
            std::vector<std::string> curr;
            size_t group_size;
            std::memcpy(&group_size, serialized.data + curr_offset, sizeof(size_t)); // 当前group size
            curr_offset += sizeof(size_t);
            for (int j = 0; j < group_size; j++) {
                size_t cell_size;
                std::memcpy(&cell_size, serialized.data + curr_offset, sizeof(size_t)); // 字符串size
                curr_offset += sizeof(size_t);
                std::string str(serialized.data + curr_offset, cell_size);
                curr_offset+= cell_size;
                curr.emplace_back(std::move(str));
            }
            groups.emplace_back(std::move(curr));
        }
    }

    void finalize_to_result(ArrayColumn* dst) const {
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
        const auto& struct_column = down_cast<StructColumn*>(ColumnHelper::get_data_column(dst->elements_column().get()))->fields_column();
        auto* time_column = down_cast<TimestampColumn*>(ColumnHelper::get_data_column(struct_column[0].get()));
        auto* interval_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(struct_column[1].get()));
        auto* array_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(ColumnHelper::get_data_column(struct_column[2].get())));
        auto* element_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(array_column->elements_column().get()));
        auto* group_cell_nullable = down_cast<NullableColumn*>(array_column->elements_column().get());
        size_t size = 0;
        if (is_self_case) {
            finalize_self_case(indices, size, time_column, interval_column, array_column, element_column, group_cell_nullable);
        } else {
            finalize_non_self_case(indices, size, time_column, interval_column, array_column, element_column, group_cell_nullable);
        }
        handle_struct_nullable(dst, size);
        dst->offsets_column()->append(dst->offsets_column()->get_data().back() + size);
    }

    void finalize_to_dst(TimestampColumn* time_column,
                         Int64Column* interval_column,
                         ArrayColumn* array_column,
                         BinaryColumn* element_column,
                         NullableColumn* group_cell_nullable,
                         std::vector<std::string>& group,
                         size_t& size, int64_t time, int64_t interval) const {
        int64_t unix_time = TaFunctions::millisToMicros(time);
        int64_t second = unix_time / 1000000;
        int64_t micro = unix_time % 1000000;
        TimestampValue to_add;
        to_add.from_unixtime(second, micro, cctz::utc_time_zone());
        time_column->append(std::move(to_add));
        interval_column->append(get_output_interval(interval));
        size++;
        size_t group_array_offset = array_column->offsets().get_data().back();
        if (!group.empty()) {
            for (size_t i = 0; i < group.size(); i++) {
                if (!group[i].empty()) {
                    if (array_column->elements_column()->is_nullable()) {
                        group_cell_nullable->null_column()->append(0);
                    }
                    element_column->append(group[i]);
                    group_array_offset++;
                } else {
                    if (array_column->elements_column()->is_nullable()) {
                        group_cell_nullable->append_nulls(1);
                    } else {
                        element_column->append_default();
                    }
                }
            }
        } else {
            group_cell_nullable->append_nulls(groups_num); // 该group array项为null
        }
        array_column->offsets_column()->append(group_array_offset);
    }

    void handle_struct_nullable(ArrayColumn* dst, size_t size) const {
        const auto& struct_column = down_cast<StructColumn*>(ColumnHelper::get_data_column(dst->elements_column().get()))->fields_column();

        if (dst->elements_column()->is_nullable()) {
            auto* nullableColumn = down_cast<NullableColumn*>(dst->elements_column().get());
            nullableColumn->null_column()->get_data().resize(nullableColumn->null_column()->get_data().size() + size);
        }
        // array里struct每个元素都不为null
        for (const auto& field : struct_column) {
            auto* nullableColumn = down_cast<NullableColumn*>(field.get());
            nullableColumn->null_column()->get_data().resize(nullableColumn->null_column()->get_data().size() + size);
        }
    }

    inline std::vector<std::string> handle_group(size_t index) const {
        // if self_case, event_id vector is empty, we need to set each event id to event_type_start
        // so event_id empty is equal to event_type_start
        bool need_handle_group = event_id.empty() || (!event_id.empty() && event_id[index] == IntervalStateConfig::event_type_start);
        if (need_handle_group) {
            return group_const ? std::move(groups[0]) : std::move(groups[index]);
        } else {
            return {};
        }
    }

    void finalize_self_case(std::vector<size_t>& indices,
                            size_t& size,
                            TimestampColumn* time_column,
                            Int64Column* interval_column,
                            ArrayColumn* array_column,
                            BinaryColumn* element_column,
                            NullableColumn* group_cell_nullable) const {
        int64_t start_time = -1;
        std::vector<std::string> start_group;

        for (size_t index : indices) {
            if (start_time == -1) {
                start_time = event_time[index];
                start_group = handle_group(index);
            } else {
                if (start_time > 0) {
                    int64_t interval = event_time[index] - start_time;
                    // AA case interval 不允许为0
                    if (interval > 0 && interval < window_gaps) {
                        finalize_to_dst(time_column, interval_column, array_column, element_column, group_cell_nullable, start_group, size, start_time, interval);
                    }
                    start_time = event_time[index];
                    start_group = handle_group(index);
                }
            }
        }
    }

    void finalize_non_self_case(std::vector<size_t>& indices,
                                size_t& size,
                                TimestampColumn* time_column,
                                Int64Column* interval_column,
                                ArrayColumn* array_column,
                                BinaryColumn* element_column,
                                NullableColumn* group_cell_nullable) const {
        int64_t start_time = -1;
        std::vector<std::string> start_group;

        for (size_t index : indices) {
            if (event_id[index] == IntervalStateConfig::event_type_start) {
                start_time = event_time[index];
                start_group = handle_group(index);
            } else {
                int64_t interval = event_time[index] - start_time;
                //如果为0，则跳过该次B事件
                if (interval <= 0) {
                    continue;
                } else if (interval <= window_gaps) {
                    if (start_time > 0) {
                        finalize_to_dst(time_column, interval_column, array_column, element_column, group_cell_nullable, start_group, size, start_time, interval);
                    }
                }
                start_time = -1;
            }
        }
    }

    void serialize_to_intermediate(BinaryColumn* dst) const override {
        Bytes& bytes = dst->get_bytes();
        size_t curr_offset = bytes.size();
        size_t new_byte_size = curr_offset + sizeof(size_t); // 行数

        new_byte_size += sizeof(uint64_t) * event_time.size(); // event_time size
        if (!is_self_case) {
            new_byte_size += sizeof(int32_t) * event_id.size(); // event_id size
        }
        for (const auto& group : groups) {
            new_byte_size += sizeof(size_t); // 当前group size
            for (const auto& g : group) {
                new_byte_size += sizeof(size_t) + g.size(); // 每个 varchar 长度 + 字符串长度
            }
        }

        // Resize bytes only once
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

        // Serialize groups
        for (const auto& group : groups) {
            size_t group_size = group.size();
            std::memcpy(bytes.data() + curr_offset, &group_size, sizeof(size_t));
            curr_offset += sizeof(size_t);

            for (const auto& g : group) {
                size_t cell_size = g.size();
                std::memcpy(bytes.data() + curr_offset, &cell_size, sizeof(size_t));
                curr_offset += sizeof(size_t);
                std::memcpy(bytes.data() + curr_offset, g.data(), cell_size);
                curr_offset += cell_size;
            }
        }

        // Store final offset
        dst->get_offset().emplace_back(curr_offset);
    }
};

class IntervalGroupAggFunction final
        : public AggregateFunctionBatchHelper<IntervalGroupState, IntervalGroupAggFunction> {
public:
    void init_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state) const {
        init_if_necessary(ctx, this->data(state));
    }

    void init_if_necessary(FunctionContext* ctx, IntervalGroupState& state) const {
        if (state.is_init) {
            return;
        }
        size_t const_num = ctx->get_num_constant_columns();
        int64_t interval_unit = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - 1).get()))->get_data()[0];
        int64_t window_gaps = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - 2).get()))->get_data()[0];
        int64_t flag = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - 3).get()))->get_data()[0];
        int64_t group_num;
        bool group_const = false;
        std::vector<std::string> group;
        //todo(zhangchen) hack for check ctx const columns contains groups
        if (const_num == 7) {
            group_num = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - 5).get()))->get_data()[0];
            if (ctx->is_constant_column(const_num - 4)) {
                const auto* group_column = down_cast<const ArrayColumn*> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - 4).get()));
                const auto& viewer = ColumnViewer<TYPE_VARCHAR>(group_column->elements_column());
                for (int i = 0; i < viewer.size(); i++) {
                    if (viewer.is_null(i)) {
                        group.emplace_back();
                    } else {
                        group.emplace_back(viewer.value(i).to_string());
                    }
                }
                group_const = true;
            }
        } else {
            group_num = down_cast<const Int64Column *> (ColumnHelper::get_data_column(ctx->get_constant_column(const_num - 4).get()))->get_data()[0];
        }
        state.init(interval_unit, flag, window_gaps, group_num, group_const, group);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if ((columns[1]->is_nullable() && columns[1]->is_null(row_num)) || columns[1]->only_null()) {
            return;
        }
        init_if_necessary(ctx, state);

        const auto* id_column = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[0]));
        const auto* time_column = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(columns[1]));
        const auto* group_column =  down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[3]));
        this->data(state).update(id_column, time_column, group_column, row_num);
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
            auto dst = down_cast<NullableColumn*>(to);
            dst->null_column()->append(0);
        }
        this->data(state).finalize_to_result(down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to)));
    }


    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {

        auto* dst_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(dst->get()));
        dst_column->reserve(chunk_size);

        const auto* id_column = down_cast<const Int32Column*>(ColumnHelper::get_data_column(src[0].get()));
        const auto* time_column = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(src[1].get()));
        const auto* group_column =  down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(src[3].get()));

        for (size_t i = 0; i < chunk_size; i++) {
            IntervalGroupState state;
            init_if_necessary(ctx, state);
            state.update(id_column, time_column, group_column, i);
            state.serialize_to_intermediate(down_cast<BinaryColumn*>(ColumnHelper::get_data_column(dst->get())));
        }
    }

    std::string get_name() const override { return "build_user_interval_agg_grouped"; }
};

}   // namespace starrocks