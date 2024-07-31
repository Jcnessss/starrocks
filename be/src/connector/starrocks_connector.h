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

#include "connector.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "exec/starrocks/starrocks_reader.h"
namespace starrocks::connector {

class StarrocksConnector final : public Connector {
public:
    ~StarrocksConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::STARROCKS; }
};
class StarrocksDataSource;
class StarrocksDataSourceProvider;

class StarrocksDataSourceProvider final : public DataSourceProvider {
public:
    ~StarrocksDataSourceProvider() override = default;
    friend class StarrocksDataSource;
    StarrocksDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

    friend class StarrocksDataSource;

protected:
    ConnectorScanNode* _scan_node;
    const TStarrocksScanNode _starrocks_scan_node;
};

class StarrocksDataSource final : public DataSource {
public:
    ~StarrocksDataSource() override = default;

    StarrocksDataSource(const StarrocksDataSourceProvider* provider, const TScanRange& scan_range);
    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

    void _init_counter();

    Status _create_scanner();
    Status initialize_src_chunk(ChunkPtr* chunk);
    Status append_batch_to_src_chunk(ChunkPtr* chunk);
    Status finalize_src_chunk(ChunkPtr* chunk);
    Status new_column(const arrow::DataType* arrow_type, const SlotDescriptor* slot_desc, ColumnPtr* column,
                      ConvertFuncTree* conv_func, Expr** expr, ObjectPool& pool, bool strict_mode);

    Status build_dest(const arrow::DataType* arrow_type, const TypeDescriptor* type_desc, bool is_nullable,
                      TypeDescriptor* raw_type_desc, ConvertFuncTree* conv_func, bool& need_cast,
                      bool strict_mode);
    Status convert_array_to_column(ConvertFuncTree* conv_func, size_t num_elements,
                                   const arrow::Array* array, const ColumnPtr& column,
                                   size_t batch_start_idx, size_t chunk_start_idx, Filter* chunk_filter,
                                   ArrowConvertContext* conv_ctx);
private:
    const StarrocksDataSourceProvider* _provider;
    TStarrocksScanRange _scan_range;

    ObjectPool _pool;
    RuntimeState* _runtime_state = nullptr;
    StarrocksScanReader* _reader = nullptr;
    bool _no_data = false;
    std::shared_ptr<arrow::RecordBatch> _batch;
    std::vector<std::unique_ptr<ConvertFuncTree>> _conv_funcs;
    std::vector<Expr*> _cast_exprs;
    ArrowConvertContext _conv_ctx;
    Filter _chunk_filter;

    RuntimeProfile::Counter* _read_counter = nullptr;
    RuntimeProfile::Counter* _read_timer = nullptr;
    RuntimeProfile::Counter* _materialize_timer = nullptr;
    RuntimeProfile::Counter* _rows_read_counter = nullptr;

    int64_t _rows_read_number = 0;
    int64_t _rows_return_number = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;
};

}
