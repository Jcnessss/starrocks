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

#include "starrocks_connector.h"

#include "column/column_helper.h"
#include "connector.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
namespace starrocks::connector {

DataSourceProviderPtr StarrocksConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                               const TPlanNode& plan_node) const {
    return std::make_unique<StarrocksDataSourceProvider>(scan_node, plan_node);
}

// ================================

StarrocksDataSourceProvider::StarrocksDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _starrocks_scan_node(plan_node.starrocks_scan_node) {}

DataSourcePtr StarrocksDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<StarrocksDataSource>(this, scan_range);
}

const TupleDescriptor* StarrocksDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_starrocks_scan_node.tuple_id);
}

// ================================

StarrocksDataSource::StarrocksDataSource(const StarrocksDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.starrocks_scan_range) {}

std::string StarrocksDataSource::name() const {
    return "StarrocksDataSource";
}

Status StarrocksDataSource::open(RuntimeState* state) {
    _runtime_state = state;
    const TStarrocksScanNode& starrocks_scan_node = _provider->_starrocks_scan_node;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(starrocks_scan_node.tuple_id);
    DCHECK(_tuple_desc != nullptr);

    const std::vector<SlotDescriptor*> slots = _tuple_desc->slots();

    RETURN_IF_ERROR(_create_scanner());

    for (auto i = 0; i < _reader->_selected_fields.size(); ++i) {
        _conv_funcs.emplace_back(std::make_unique<ConvertFuncTree>());
    }
    _cast_exprs.resize(_reader->_selected_fields.size(), nullptr);
    _chunk_filter.reserve(_reader->_batch_size);
    _init_counter();
    _conv_ctx.state = state;
    return Status::OK();
}

Status StarrocksDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    if (_no_data) {
        return Status::EndOfFile("no data");
    }
    ChunkPtr tmp_chunk;
    RETURN_IF_ERROR(_reader->get_next(&_batch));
    RETURN_IF_ERROR(initialize_src_chunk(&tmp_chunk));
    RETURN_IF_ERROR(append_batch_to_src_chunk(&tmp_chunk));
    RETURN_IF_ERROR(finalize_src_chunk(&tmp_chunk));
    *chunk = tmp_chunk;
    return Status::OK();
}

Status StarrocksDataSource::_create_scanner() {
    _reader = _pool.add(new StarrocksScanReader(
            _scan_range.remote_be_host,
            _scan_range.remote_be_port,
            _provider->_starrocks_scan_node.username,
            _provider->_starrocks_scan_node.passwd,
            _provider->_starrocks_scan_node.query_plan,
            _provider->_starrocks_scan_node.table_name,
            _provider->_starrocks_scan_node.database,
            _scan_range.tablet_ids));
    RETURN_IF_ERROR(_reader->open());
    return Status::OK();
}

void StarrocksDataSource::_init_counter() {
    _read_counter = ADD_COUNTER(_runtime_profile, "ReadCounter", TUnit::UNIT);
    _rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_runtime_profile, "TotalRawReadTime(*)");
    _materialize_timer = ADD_TIMER(_runtime_profile, "MaterializeTupleTime(*)");
}

Status StarrocksDataSource::initialize_src_chunk(ChunkPtr* chunk) {
    (*chunk) = std::make_shared<Chunk>();
    size_t column_pos = 0;
    for (auto i = 0; i < _reader->_selected_fields.size(); ++i) {
        SlotDescriptor* slot_desc = _tuple_desc->slots()[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto* array = _batch->column(column_pos++).get();
        ColumnPtr column;
        RETURN_IF_ERROR(new_column(array->type().get(), slot_desc, &column, _conv_funcs[i].get(), &_cast_exprs[i],
                                   _pool, false));
        column->reserve(_batch->num_rows());
        (*chunk)->append_column(column, slot_desc->id());
    }
    return Status::OK();
}

Status StarrocksDataSource::append_batch_to_src_chunk(ChunkPtr* chunk) {
    size_t column_pos = 0;
    for (auto i = 0; i < _reader->_selected_fields.size(); ++i) {
        SlotDescriptor* slot_desc = _tuple_desc->slots()[i];
        if (slot_desc == nullptr) {
            continue;
        }
        _conv_ctx.current_slot = slot_desc;
        auto* array = _batch->column(column_pos++).get();
        auto& column = (*chunk)->get_column_by_slot_id(slot_desc->id());
        // for timestamp type, _state->timezone which is specified by user. convert function
        // obtains timezone from array. thus timezone in array should be rectified to
        // _state->timezone.
        if (array->type_id() == ArrowTypeId::TIMESTAMP) {
            auto* timestamp_type = down_cast<arrow::TimestampType*>(array->type().get());
            auto& mutable_timezone = (std::string&)timestamp_type->timezone();
            mutable_timezone = _runtime_state->timezone();
        }
        RETURN_IF_ERROR(convert_array_to_column(_conv_funcs[i].get(), _batch->num_rows(), array, column, 0,
                                                0, &_chunk_filter, &_conv_ctx));
    }

    return Status::OK();
}

Status StarrocksDataSource::finalize_src_chunk(ChunkPtr* chunk) {
    ChunkPtr cast_chunk = std::make_shared<Chunk>();
    {
        if (VLOG_ROW_IS_ON) {
            VLOG_ROW << "before finalize chunk: " << (*chunk)->debug_columns();
        }
        for (auto i = 0; i < _reader->_selected_fields.size(); ++i) {
            SlotDescriptor* slot_desc = _tuple_desc->slots()[i];
            if (slot_desc == nullptr) {
                continue;
            }

            ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, (*chunk).get()));
            column = ColumnHelper::unfold_const_column(slot_desc->type(), (*chunk)->num_rows(), column);
            cast_chunk->append_column(column, slot_desc->id());
        }
        if (VLOG_ROW_IS_ON) {
            VLOG_ROW << "after finalize chunk: " << cast_chunk->debug_columns();
        }
    }
    *chunk = cast_chunk;
    return Status::OK();
}
Status StarrocksDataSource::new_column(const arrow::DataType* arrow_type, const SlotDescriptor* slot_desc, ColumnPtr* column,
                                  ConvertFuncTree* conv_func, Expr** expr, ObjectPool& pool, bool strict_mode) {
    auto& type_desc = slot_desc->type();
    auto* raw_type_desc = pool.add(new TypeDescriptor());
    bool need_cast = false;
    RETURN_IF_ERROR(build_dest(arrow_type, &type_desc, slot_desc->is_nullable(), raw_type_desc, conv_func, need_cast,
                               strict_mode));
    if (!need_cast) {
        *expr = pool.add(new ColumnRef(slot_desc));
        (*column) = ColumnHelper::create_column(type_desc, slot_desc->is_nullable());
    } else {
        auto slot = pool.add(new ColumnRef(slot_desc));
        *expr = VectorizedCastExprFactory::from_type(*raw_type_desc, slot_desc->type(), slot, &pool);
        if ((*expr) == nullptr) {
            return illegal_converting_error(arrow_type->name(), type_desc.debug_string());
        }
        *column = ColumnHelper::create_column(*raw_type_desc, slot_desc->is_nullable());
    }

    return Status::OK();
}

Status StarrocksDataSource::build_dest(const arrow::DataType* arrow_type, const TypeDescriptor* type_desc, bool is_nullable,
                                  TypeDescriptor* raw_type_desc, ConvertFuncTree* conv_func, bool& need_cast,
                                  bool strict_mode) {
    auto at = arrow_type->id();
    auto lt = type_desc->type;
    conv_func->func = get_arrow_converter(at, lt, is_nullable, strict_mode);
    conv_func->children.clear();

    switch (lt) {
    case TYPE_ARRAY: {
        if (at != ArrowTypeId::LIST && at != ArrowTypeId::LARGE_LIST && at != ArrowTypeId::FIXED_SIZE_LIST) {
            return Status::InternalError(
                    fmt::format("Apache Arrow type (nested) {} does not match the type {} in StarRocks",
                                arrow_type->name(), type_to_string(lt)));
        }
        raw_type_desc->type = TYPE_ARRAY;
        TypeDescriptor type;
        auto cf = std::make_unique<ConvertFuncTree>();
        auto sub_at = arrow_type->field(0)->type();
        RETURN_IF_ERROR(
                build_dest(sub_at.get(), &type_desc->children[0], true, &type, cf.get(), need_cast, strict_mode));
        raw_type_desc->children.emplace_back(std::move(type));
        conv_func->children.emplace_back(std::move(cf));
        break;
    }
    case TYPE_MAP: {
        if (at != ArrowTypeId::MAP) {
            return Status::InternalError(
                    fmt::format("Apache Arrow type (nested) {} does not match the type {} in StarRocks",
                                arrow_type->name(), type_to_string(lt)));
        }

        raw_type_desc->type = TYPE_MAP;
        for (auto i = 0; i < 2; i++) {
            TypeDescriptor type;
            auto cf = std::make_unique<ConvertFuncTree>();
            auto sub_at = i == 0 ? down_cast<const arrow::MapType*>(arrow_type)->key_type()
                                 : down_cast<const arrow::MapType*>(arrow_type)->item_type();
            RETURN_IF_ERROR(
                    build_dest(sub_at.get(), &type_desc->children[i], true, &type, cf.get(), need_cast, strict_mode));
            raw_type_desc->children.emplace_back(std::move(type));
            conv_func->children.emplace_back(std::move(cf));
        }
        break;
    }
    case TYPE_STRUCT: {
        if (at != ArrowTypeId::STRUCT) {
            return Status::InternalError(
                    fmt::format("Apache Arrow type (nested) {} does not match the type {} in StarRocks",
                                arrow_type->name(), type_to_string(lt)));
        }
        auto field_size = type_desc->children.size();
        auto arrow_field_size = arrow_type->num_fields();

        raw_type_desc->type = TYPE_STRUCT;
        raw_type_desc->field_names = type_desc->field_names;
        conv_func->field_names = type_desc->field_names;
        for (auto i = 0; i < field_size; i++) {
            TypeDescriptor type;
            auto cf = std::make_unique<ConvertFuncTree>();
            auto sub_at = i >= arrow_field_size ? arrow::null() : arrow_type->field(i)->type();
            RETURN_IF_ERROR(
                    build_dest(sub_at.get(), &type_desc->children[i], true, &type, cf.get(), need_cast, strict_mode));
            raw_type_desc->children.emplace_back(std::move(type));
            conv_func->children.emplace_back(std::move(cf));
        }
        break;
    }
    default: {
        if (conv_func->func == nullptr) {
            need_cast = true;
            Status error = illegal_converting_error(arrow_type->name(), type_desc->debug_string());
            auto strict_pt = get_strict_type(at);
            if (strict_pt == TYPE_UNKNOWN) {
                return error;
            }
            auto strict_conv_func = get_arrow_converter(at, strict_pt, is_nullable, strict_mode);
            if (strict_conv_func == nullptr) {
                return error;
            }
            conv_func->func = strict_conv_func;
            raw_type_desc->type = strict_pt;
            switch (strict_pt) {
            case TYPE_DECIMAL128: {
                const auto* discrete_type = down_cast<const arrow::Decimal128Type*>(arrow_type);
                auto precision = discrete_type->precision();
                auto scale = discrete_type->scale();
                if (precision < 1 || precision > decimal_precision_limit<int128_t> || scale < 0 || scale > precision) {
                    return Status::InternalError(
                            strings::Substitute("Decimal($0, $1) is out of range.", precision, scale));
                }
                raw_type_desc->precision = precision;
                raw_type_desc->scale = scale;
                break;
            }
            case TYPE_VARCHAR: {
                raw_type_desc->len = TypeDescriptor::MAX_VARCHAR_LENGTH;
                break;
            }
            case TYPE_CHAR: {
                raw_type_desc->len = TypeDescriptor::MAX_CHAR_LENGTH;
                break;
            }
            case TYPE_DECIMALV2:
            case TYPE_DECIMAL32:
            case TYPE_DECIMAL64: {
                return Status::InternalError(
                        strings::Substitute("Apache Arrow type($0) does not match the type($1) in StarRocks",
                                            arrow_type->name(), type_to_string(strict_pt)));
            }
            default:
                break;
            }
        } else {
            *raw_type_desc = *type_desc;
        }
    }
    }
    return Status::OK();
}

Status StarrocksDataSource::convert_array_to_column(ConvertFuncTree* conv_func, size_t num_elements,
                                                    const arrow::Array* array, const ColumnPtr& column,
                                                    size_t batch_start_idx, size_t chunk_start_idx, Filter* chunk_filter,
                                                    ArrowConvertContext* conv_ctx) {
    uint8_t* null_data;
    Column* data_column;
    if (column->is_nullable()) {
        auto nullable_column = down_cast<NullableColumn*>(column.get());
        auto null_column = nullable_column->mutable_null_column();
        size_t null_count = fill_null_column(array, batch_start_idx, num_elements, null_column, chunk_start_idx);
        nullable_column->set_has_null(null_count != 0);
        null_data = &null_column->get_data().front() + chunk_start_idx;
        data_column = nullable_column->data_column().get();
    } else {
        null_data = nullptr;
        // Fill nullable array into not-nullable column, positions of NULLs is marked as 1
        fill_filter(array, batch_start_idx, num_elements, chunk_filter, chunk_start_idx, conv_ctx);
        data_column = column.get();
    }

    auto st = conv_func->func(array, batch_start_idx, num_elements, data_column, chunk_start_idx, null_data,
                              chunk_filter, conv_ctx, conv_func);
    if (st.ok() && column->is_nullable()) {
        // in some scene such as string length exceeds limit, the column will be set NULL, so we need reset has_null
        down_cast<NullableColumn*>(column.get())->update_has_null();
    }
    return st;
}

void StarrocksDataSource::close(RuntimeState* state) {
    if (_reader != nullptr) {
        WARN_IF_ERROR(_reader->close(), "close starrocks reader failed");
    }
    _pool.clear();
}

int64_t StarrocksDataSource::raw_rows_read() const {
    return _rows_read_number;
}
int64_t StarrocksDataSource::num_rows_read() const {
    return _rows_return_number;
}
int64_t StarrocksDataSource::num_bytes_read() const {
    return _bytes_read;
}
int64_t StarrocksDataSource::cpu_time_spent() const {
    return _cpu_time_ns;
}

}
