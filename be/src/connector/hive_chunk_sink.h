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

#include <boost/thread/future.hpp>
#include <future>
#include <exec/schema_scanner.h>

#include "column/chunk.h"
#include "common/status.h"
#include "connector/async_flush_stream_poller.h"
#include "connector/connector.h"
#include "connector/sink_memory_manager.h"
#include "connector_chunk_sink.h"
#include "formats/column_evaluator.h"
#include "formats/file_writer.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"
#include "utils.h"

namespace starrocks::connector {

class HiveChunkSink : public ConnectorChunkSink {
public:
    HiveChunkSink(std::vector<std::string> partition_columns,
                  std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                  std::unique_ptr<LocationProvider> location_provider,
                  std::unique_ptr<formats::FileWriterFactory> file_writer_factory, int64_t max_file_size,
                  RuntimeState* state);

    ~HiveChunkSink() override = default;

    void callback_on_commit(const CommitResult& result) override;
};

struct HiveChunkSinkSchemaScanContext {
    std::string catalog;
    std::string database;
    std::string table;
    std::string ip;
    std::int32_t port;
    std::int64_t query_timeout_ms;
    std::string sink_partition_location;
};

struct HiveChunkSinkContext : public ConnectorChunkSinkContext {
    ~HiveChunkSinkContext() override = default;

    std::string path;
    std::vector<std::string> data_column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> data_column_evaluators;
    std::vector<std::string> partition_column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators;
    int64_t max_file_size = 128L * 1024 * 1024;
    std::string format;
    TCompressionType::type compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    std::map<std::string, std::string> options;
    PriorityThreadPool* executor = nullptr;
    TCloudConfiguration cloud_conf;
    pipeline::FragmentContext* fragment_context = nullptr;
    HiveChunkSinkSchemaScanContext schema_scan_ctx;
};

class HiveChunkSinkProvider : public ConnectorChunkSinkProvider {
public:
    ~HiveChunkSinkProvider() override = default;

    StatusOr<std::unique_ptr<ConnectorChunkSink>> create_chunk_sink(std::shared_ptr<ConnectorChunkSinkContext> context,
                                                                    int32_t driver_id) override;
};


class HiveChunkSinkLocationProvider : public LocationProvider {
public:
    HiveChunkSinkLocationProvider(const std::string& base_path, const std::string& query_id, int be_number,
        int driver_id, const std::string& file_suffix, const HiveChunkSinkSchemaScanContext& ctx)
        : LocationProvider(base_path, query_id, be_number, driver_id, file_suffix) {
        _schema_scan_ctx = ctx;
    }

    ~HiveChunkSinkLocationProvider() override = default;

    std::string get(const std::string& partition) override;

    StatusOr<std::string> get_partition_location(const std::string& partition);

private:
    HiveChunkSinkSchemaScanContext _schema_scan_ctx;
    TGetPartitionsMetaResponse _partitions_meta_response;
    std::map<std::string, std::string> _partition2location;
};

} // namespace starrocks::connector
