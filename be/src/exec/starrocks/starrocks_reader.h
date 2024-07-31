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

#include <arrow/record_batch.h>

#include "common/status.h"
#include "gen_cpp/build/gen_cpp/StarrocksExternalService_types.h"
namespace starrocks {

class StarrocksScanReader {
public:
    StarrocksScanReader(const std::string& remote_be_host, const std::string& remote_be_port,
                        const std::string& username, const std::string& passwd,
                        const std::string& query_plan, const std::string& table,
                        const std::string& database, const std::vector<long> tablet_ids);
    ~StarrocksScanReader();

    Status open();
    Status get_next(std::shared_ptr<arrow::RecordBatch>* record_batch);
    Status close();

    std::vector<TScanColumnDesc> _selected_fields;
    int _batch_size;
private:
    std::string _remote_be_host;
    std::string _remote_be_port;
    std::string _username;
    std::string _passwd;
    std::string _query_plan;
    std::string _table;
    std::string _database;
    std::vector<long> _tablet_ids;
    int _timeout_ms;
    std::string _context_id;
    int64_t _offset;
};

}
