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

#include "starrocks_reader.h"

#include <arrow/record_batch.h>

#include "gen_cpp/build/gen_cpp/TStarrocksExternalService.h"
#include "runtime/client_cache.h"
#include "util/arrow/row_batch.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

StarrocksScanReader::StarrocksScanReader(const std::string& remote_be_host, const std::string& remote_be_port,
                                         const std::string& username, const std::string& passwd,
                                         const std::string& query_plan, const std::string& table,
                                         const std::string& database, const std::vector<long> tablet_ids,
                                         const int batch_size)
        : _batch_size(batch_size),
          _remote_be_host(remote_be_host),
          _remote_be_port(remote_be_port),
          _username(username),
          _passwd(passwd),
          _query_plan(query_plan),
          _table(table),
          _database(database),
          _tablet_ids(tablet_ids) {
    _timeout_ms = 30000;
    _offset = 0;
}

StarrocksScanReader::~StarrocksScanReader() = default;

Status StarrocksScanReader::open() {
    TScanOpenParams tScanOpenParams;
    tScanOpenParams.__set_user(_username);
    tScanOpenParams.__set_passwd(_passwd);
    tScanOpenParams.__set_table(_table);
    tScanOpenParams.__set_database(_database);
    tScanOpenParams.__set_tablet_ids(_tablet_ids);
    tScanOpenParams.__set_opaqued_query_plan(_query_plan);
    tScanOpenParams.__set_batch_size(_batch_size);
    tScanOpenParams.__set_mem_limit(-1);
    tScanOpenParams.__set_query_timeout(3600);
    TScanOpenResult tScanOpenResult;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<TStarrocksExternalServiceClient>(
            _remote_be_host, std::stoi(_remote_be_port),
            [&tScanOpenResult, &tScanOpenParams](StarrocksExternalServiceConnection& client) {
                client->open_scanner(tScanOpenResult, tScanOpenParams);
            },
            _timeout_ms));
    if (tScanOpenResult.status.status_code == TStatusCode::OK) {
        _selected_fields = tScanOpenResult.selected_columns;
        _context_id = tScanOpenResult.context_id;
    }
    return Status(tScanOpenResult.status);
}

Status StarrocksScanReader::get_next() {
    TScanNextBatchParams tScanNextBatchParams;
    tScanNextBatchParams.__set_context_id(_context_id);
    tScanNextBatchParams.__set_offset(_offset);
    TScanBatchResult tScanBatchResult;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<TStarrocksExternalServiceClient>(
            _remote_be_host, std::stoi(_remote_be_port),
            [&tScanBatchResult, &tScanNextBatchParams](StarrocksExternalServiceConnection& client) {
                client->get_next(tScanBatchResult, tScanNextBatchParams);
            },
            _timeout_ms));
    if (tScanBatchResult.status.status_code != TStatusCode::OK) {
        return Status(tScanBatchResult.status);
    }
    if (tScanBatchResult.eos) {
        return Status::EndOfFile("");
    }
    _batch.reset();
    _rows.reset();
    _rows = std::make_shared<std::string>(tScanBatchResult.rows);
    RETURN_IF_ERROR(deserialize_record_batch(&_batch, *_rows.get()));
    _offset += _batch->num_rows();
    return Status::OK();
}

Status StarrocksScanReader::close() {
    TScanCloseParams tScanCloseParams;
    tScanCloseParams.__set_context_id(_context_id);
    TScanCloseResult tScanCloseResult;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<TStarrocksExternalServiceClient>(
            _remote_be_host, std::stoi(_remote_be_port),
            [&tScanCloseResult, &tScanCloseParams](StarrocksExternalServiceConnection& client) {
                client->close_scanner(tScanCloseResult, tScanCloseParams);
            },
            _timeout_ms));
    return Status(tScanCloseResult.status);
}

}