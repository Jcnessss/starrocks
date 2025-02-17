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

#include "csv_file_writer.h"

#include <utility>

#include "formats/utils.h"
#include "output_stream_file.h"
#include "runtime/current_thread.h"

namespace starrocks::formats {

CSVFileWriter::CSVFileWriter(std::string location, std::shared_ptr<csv::OutputStream> output_stream,
                             std::vector<std::string> column_names, std::vector<TypeDescriptor> types,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                             TCompressionType::type compression_type, std::shared_ptr<CSVWriterOptions> writer_options,
                             std::function<void()> rollback_action, std::string partition_name)
        : _location(std::move(location)),
          _output_stream(std::move(output_stream)),
          _column_names(std::move(column_names)),
          _types(std::move(types)),
          _column_evaluators(std::move(column_evaluators)),
          _compression_type(compression_type),
          _writer_options(std::move(writer_options)),
          _rollback_action(std::move(rollback_action)),
          _partition_name(std::move(partition_name)) {}

CSVFileWriter::~CSVFileWriter() = default;

Status CSVFileWriter::init() {
    if (_compression_type != TCompressionType::NO_COMPRESSION) {
        return Status::NotSupported(fmt::format("not supported compression type {}", to_string(_compression_type)));
    }

    RETURN_IF_ERROR(ColumnEvaluator::init(_column_evaluators));
    _column_converters.reserve(_types.size());
    for (auto& type : _types) {
        // TODO: support nested type of hive
        if (type.is_complex_type()) {
            return Status::InternalError(fmt::format("Type {} is not supported yet", type.debug_string()));
        }
        auto nullable_conv = csv::get_converter(type, true);
        if (nullable_conv == nullptr) {
            return Status::InternalError(fmt::format("No CSV converter for type: {}", type.debug_string()));
        }
        _column_converters.emplace_back(std::move(nullable_conv), csv::get_converter(type, false));
    }
    return Status::OK();
}

int64_t CSVFileWriter::get_written_bytes() {
    return _output_stream->size();
}

int64_t CSVFileWriter::get_allocated_bytes() {
    return _output_stream->buffer_size();
}

Status CSVFileWriter::write(Chunk* chunk) {
    _num_rows += chunk->num_rows();

    auto columns = std::vector<ColumnPtr>();
    for (auto& e : _column_evaluators) {
        ASSIGN_OR_RETURN(auto column, e->evaluate(chunk));
        columns.push_back(std::move(column));
    }

    for (size_t r = 0; r < chunk->num_rows(); r++) {
        for (size_t c = 0; c < columns.size(); c++) {
            csv::Converter* converter;
            if (columns[c]->is_nullable()) {
                converter = _column_converters[c].first.get();
            } else {
                converter = _column_converters[c].second.get();
            }
            RETURN_IF_ERROR(converter->write_string(_output_stream.get(), *columns[c], r, {}));
            if (c + 1 != columns.size()) {
                RETURN_IF_ERROR(_output_stream->write(_writer_options->column_terminated_by));
            }
        }
        RETURN_IF_ERROR(_output_stream->write(_writer_options->line_terminated_by));
    }

    return Status::OK();
}

FileWriter::CommitResult CSVFileWriter::commit() {
    FileWriter::CommitResult result{
            .io_status = Status::OK(), .format = CSV, .location = _location, .rollback_action = _rollback_action,
            .partition_name = _partition_name};

    if (auto st = _output_stream->finalize(); !st.ok()) {
        result.io_status.update(st);
    }

    if (result.io_status.ok()) {
        result.file_statistics.record_count = _num_rows;
        result.file_statistics.file_size = _output_stream->size();
    }

    return result;
}

CSVFileWriterFactory::CSVFileWriterFactory(std::shared_ptr<FileSystem> fs, TCompressionType::type compression_type,
                                           std::map<std::string, std::string> options,
                                           std::vector<std::string> column_names,
                                           std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                                           PriorityThreadPool* executors, RuntimeState* runtime_state, FSOptions fs_options)
        : _fs(std::move(fs)),
          _compression_type(compression_type),
          _options(std::move(options)),
          _column_names(std::move(column_names)),
          _column_evaluators(std::move(column_evaluators)),
          _executors(executors),
          _runtime_state(runtime_state),
          _fs_options(fs_options) {}

Status CSVFileWriterFactory::init() {
    for (auto& e : _column_evaluators) {
        RETURN_IF_ERROR(e->init());
    }
    _parsed_options = std::make_shared<CSVWriterOptions>();
    if (_options.contains(CSVWriterOptions::COLUMN_TERMINATED_BY)) {
        _parsed_options->column_terminated_by = _options[CSVWriterOptions::COLUMN_TERMINATED_BY];
    }
    if (_options.contains(CSVWriterOptions::LINE_TERMINATED_BY)) {
        _parsed_options->line_terminated_by = _options[CSVWriterOptions::LINE_TERMINATED_BY];
    }
    return Status::OK();
}

StatusOr<WriterAndStream> CSVFileWriterFactory::create(const std::string& path, const std::string& partition_name) const {
    std::shared_ptr<FileSystem> curr_fs;
    auto type = FileSystem::parse_type(path);
    if (type == _fs->type()) {
        curr_fs = _fs;
    } else {
        auto it = _other_type_to_fs.find(type);
        if (it != _other_type_to_fs.end()) {
            curr_fs = it->second;
        } else {
            if (_fs_options.cloud_configuration == nullptr) {
                return Status::InvalidArgument(fmt::format("need cloud config for create path {}", path));
            }
            _other_type_to_fs[type] = std::move(FileSystem::CreateUniqueFromString(path, _fs_options).value());
            curr_fs = _other_type_to_fs[type];
        }
    }

    ASSIGN_OR_RETURN(auto file, curr_fs->new_writable_file(WritableFileOptions{.direct_write = true}, path));
    auto rollback_action = [fs = curr_fs, path = path]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(path)), "fail to delete file");
    };
    auto column_evaluators = ColumnEvaluator::clone(_column_evaluators);
    auto types = ColumnEvaluator::types(_column_evaluators);
    auto async_output_stream =
            std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, _runtime_state);
    auto csv_output_stream = std::make_shared<csv::AsyncOutputStreamFile>(async_output_stream.get(), 1024 * 1024);
    auto writer =
            std::make_unique<CSVFileWriter>(path, csv_output_stream, _column_names, types, std::move(column_evaluators),
                                            _compression_type, _parsed_options, rollback_action, partition_name);
    return WriterAndStream{
            .writer = std::move(writer),
            .stream = std::move(async_output_stream),
    };
}

} // namespace starrocks::formats
