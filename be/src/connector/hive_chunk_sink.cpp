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

#include "hive_chunk_sink.h"

#include <future>
#include <exec/schema_scanner/schema_helper.h>
#include <opentelemetry/ext/http/client/http_client.h>

#include "exec/pipeline/fragment_context.h"
#include "exprs/expr.h"
#include "formats/csv/csv_file_writer.h"
#include "formats/orc/orc_file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/utils.h"
#include "util/url_coding.h"
#include "utils.h"

namespace starrocks::connector {

HiveChunkSink::HiveChunkSink(std::vector<std::string> partition_columns,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                             std::unique_ptr<LocationProvider> location_provider,
                             std::unique_ptr<formats::FileWriterFactory> file_writer_factory, int64_t max_file_size,
                             RuntimeState* state)
        : ConnectorChunkSink(std::move(partition_columns), std::move(partition_column_evaluators),
                             std::move(location_provider), std::move(file_writer_factory), max_file_size, state,
                             false) {}

void HiveChunkSink::callback_on_commit(const CommitResult& result) {
    _rollback_actions.push_back(std::move(result.rollback_action));
    if (result.io_status.ok()) {
        _state->update_num_rows_load_sink(result.file_statistics.record_count);
        THiveFileInfo hive_file_info;
        hive_file_info.__set_file_name(PathUtils::get_filename(result.location));
        hive_file_info.__set_partition_path(PathUtils::get_parent_path(result.location));
        hive_file_info.__set_record_count(result.file_statistics.record_count);
        hive_file_info.__set_file_size_in_bytes(result.file_statistics.file_size);
        hive_file_info.__set_partition_name(result.partition_name);
        TSinkCommitInfo commit_info;
        commit_info.__set_hive_file_info(hive_file_info);
        _state->add_sink_commit_info(commit_info);
    }
}

StatusOr<std::unique_ptr<ConnectorChunkSink>> HiveChunkSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<HiveChunkSinkContext>(context);
    auto runtime_state = ctx->fragment_context->runtime_state();
    auto fs = FileSystem::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_conf)).value(); // must succeed
    auto data_column_evaluators = ColumnEvaluator::clone(ctx->data_column_evaluators);
    auto location_provider = std::make_unique<connector::HiveChunkSinkLocationProvider>(
            ctx->path, print_id(ctx->fragment_context->query_id()), runtime_state->be_number(), driver_id,
            boost::to_lower_copy(ctx->format), ctx->schema_scan_ctx);

    std::unique_ptr<formats::FileWriterFactory> file_writer_factory;
    if (boost::iequals(ctx->format, formats::PARQUET)) {
        // ensure hive compatibility since hive 3 and lower version accepts specific encoding
        ctx->options[formats::ParquetWriterOptions::USE_LEGACY_DECIMAL_ENCODING] = "true";
        ctx->options[formats::ParquetWriterOptions::USE_INT96_TIMESTAMP_ENCODING] = "true";
        file_writer_factory = std::make_unique<formats::ParquetFileWriterFactory>(
                std::move(fs), ctx->compression_type, ctx->options, ctx->data_column_names,
                std::move(data_column_evaluators), std::nullopt, ctx->executor, runtime_state, FSOptions(&ctx->cloud_conf));
    } else if (boost::iequals(ctx->format, formats::ORC)) {
        file_writer_factory = std::make_unique<formats::ORCFileWriterFactory>(
                std::move(fs), ctx->compression_type, ctx->options, ctx->data_column_names,
                std::move(data_column_evaluators), ctx->executor, runtime_state, FSOptions(&ctx->cloud_conf));
    } else if (boost::iequals(ctx->format, formats::TEXTFILE)) {
        file_writer_factory = std::make_unique<formats::CSVFileWriterFactory>(
                std::move(fs), ctx->compression_type, ctx->options, ctx->data_column_names,
                std::move(data_column_evaluators), ctx->executor, runtime_state, FSOptions(&ctx->cloud_conf));
    } else {
        file_writer_factory = std::make_unique<formats::UnknownFileWriterFactory>(ctx->format);
    }

    auto partition_column_evaluators = ColumnEvaluator::clone(ctx->partition_column_evaluators);
    return std::make_unique<connector::HiveChunkSink>(
            ctx->partition_column_names, std::move(partition_column_evaluators), std::move(location_provider),
            std::move(file_writer_factory), ctx->max_file_size, runtime_state);
}

StatusOr<std::string> HiveChunkSinkLocationProvider::get_partition_location(const std::string& partition) {
    auto it = _partition2location.find(partition);
    if (it != _partition2location.end()) {
        return it->second;
    }

    // static partition insert
    if (!_schema_scan_ctx.sink_partition_location.empty()) {
        _partition2location[partition] = _schema_scan_ctx.sink_partition_location;
        return _partition2location[partition];
    }

    SchemaScannerState sst;
    sst.ip = _schema_scan_ctx.ip;
    sst.port = _schema_scan_ctx.port;
    sst.timeout_ms = _schema_scan_ctx.port;

    TGetSinkPartitionsMetaRequest request;
    request.__set_catalog(_schema_scan_ctx.catalog);
    request.__set_database(_schema_scan_ctx.database);
    request.__set_table(_schema_scan_ctx.table);
    // 当前每次只会查1个分区信息
    request.__set_partition_names({PathUtils::remove_trailing_slash(partition)});

    auto status = SchemaHelper::get_sink_partitions_meta(sst, request, &_partitions_meta_response);
    if (!status.ok()) {
        return status;
    }
    // 当前get_sink_partitions_meta rpc 仅需要查1个分区的信息，所以取partitions_meta_infos[0]即可
    if (_partitions_meta_response.partitions_meta_infos.empty() ||
        !_partitions_meta_response.partitions_meta_infos[0].__isset.storage_path) {
        return std::string{};
    }

    const auto& storage_path = _partitions_meta_response.partitions_meta_infos[0].storage_path;
    _partition2location[partition] = storage_path;

    return storage_path;
}

std::string HiveChunkSinkLocationProvider::get(const std::string& partition) {
    auto status = get_partition_location(partition);
    if (!status.ok()) {
        return {};
    }
    const auto& partition_location = status.value();
    if (partition_location.empty()) {
        // 分区不存在，fallback原来逻辑
        return LocationProvider::get(partition);
    }

    auto type = FileSystem::parse_type(partition_location);
    auto base_path_type = FileSystem::parse_type(_base_path);
    if (type == FileSystem::S3 || type != base_path_type) {
        // partition location 在 s3，由于s3不支持rename，需要直写
        // 或者
        // table location 和 partition location fs不一致，需要直写partition location
        return fmt::format("{}/{}_{}.{}", PathUtils::remove_trailing_slash(partition_location), _file_name_prefix,
                           _partition2index[partition]++, _file_name_suffix);
    } else {
        // 其余情况fallback写临时目录，由fe rename
        return LocationProvider::get(partition);
    }
}

} // namespace starrocks::connector
