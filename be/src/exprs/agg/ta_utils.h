//
// Created by Co1a on 24-11-19.
//

#pragma once

#include "column/column_viewer.h"
#include "column/column_helper.h"
#include "column/array_column.h"


namespace starrocks {
// Function: ta_map_union_greatest
struct MapUnionGreatestState {
    void init() {
        this->_data = phmap::flat_hash_map<TimestampValue,int64_t>();
        this->_init = true;
    }
    void reset() {
        this->_data.clear();
        this->_init = false;
    }
    void update(const TimestampColumn* key_column,
                const Int64Column* value_column,
                size_t start, size_t end) {
        const auto& key_column_data = key_column->get_data();
        const auto& value_column_data = value_column->get_data();
        for (size_t i = start; i < end; i++) {
            auto key = key_column_data[i];
            auto value = value_column_data[i];
            if (this->_data.contains(key)) {
                this->_data[key]= std::max(this->_data[key], value);
                continue;
            }
            this->_data[key] = value;
        }
    }
    bool _init = false;
    phmap::flat_hash_map<TimestampValue,int64_t> _data = {};
};

class MapUnionGreatestAggregateFunction final
        : public AggregateFunctionBatchHelper<MapUnionGreatestState, MapUnionGreatestAggregateFunction> {
public:

    void init_state_if_necessary([[maybe_unused]]FunctionContext* ctx, AggDataPtr __restrict state) const {
        if (this->data(state)._init) {
            return;
        }
        this->data(state).init();
    }

    void reset(FunctionContext* ctx, const Columns& arg, AggDataPtr state) const override {
            this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                    size_t row_num) const override {
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()) {
            return;
        }
        init_state_if_necessary(ctx, state);
        const auto& map_column = down_cast<const MapColumn&>(*ColumnHelper::get_data_column(columns[0]));
        auto key_column = down_cast<const TimestampColumn*>(ColumnHelper::get_data_column(map_column.keys_column().get()));
        auto value_column = down_cast<const Int64Column*>(ColumnHelper::get_data_column(map_column.values_column().get()));
        auto offset = map_column.offsets().get_data();
        auto start = offset[row_num];
        auto end = offset[row_num + 1];
        this->data(state).update(key_column, value_column, start, end);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        init_state_if_necessary(ctx, state);
        const auto&  elements = down_cast<const MapColumn&>(*ColumnHelper::get_data_column(column));
        auto offset = elements.offsets().get_data();
        auto keys = down_cast<TimestampColumn*>(ColumnHelper::get_data_column(elements.keys_column().get()));
        auto values = down_cast<Int64Column*>(ColumnHelper::get_data_column(elements.values_column().get()));
        size_t start = offset[row_num];
        size_t end = offset[row_num + 1];
        this->data(state).update(keys, values, start, end);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto dst = down_cast<MapColumn*>(ColumnHelper::get_data_column(to));
        if (this->data(state)._data.size() == 0 && to->is_nullable()) {
            auto null_column = down_cast<NullableColumn*>(to)->null_column();
            null_column->append(1);
            return;
        }

        if (to->is_nullable()) {
            auto null_column = down_cast<NullableColumn*>(to)->null_column();
            null_column->append(0);
        }
        auto key_column = down_cast<TimestampColumn*>(ColumnHelper::get_data_column(dst->keys_column().get()));
        if (dst->keys_column()->is_nullable()) {
            auto pNullableColumn = down_cast<NullableColumn*>(dst->keys_column().get());
            pNullableColumn->null_column()->append(0);
        }
        auto value_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(dst->values_column().get()));
        if (dst->values_column()->is_nullable()) {
            auto pNullableColumn = down_cast<NullableColumn*>(dst->values_column().get());
            pNullableColumn->null_column()->append(0);
        }
        auto offsets = dst->offsets_column();
        size_t element_count = 0;
        std::for_each(this->data(state)._data.begin(), this->data(state)._data.end(), [&](const auto& entry){
            key_column->append(std::move(entry.first));
            value_column->append(std::move(entry.second));
            element_count++;
        });
        offsets->append(offsets.get()->get_data().back()+element_count);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst=src[0];
    }

    std::string get_name() const override { return "ta_map_union_greatest"; }
};
}
