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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/network_util.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exprs/ip_location/base_location_service.h"
#include "util/sync_remote_file_util.h"

namespace starrocks {

void BaseLocationService::load_ip_db(std::string_view path) {
    if (!need_update_db()) {
        return;
    }
    std::lock_guard lock(mutex);
    if (!need_update_db()) {
        return;
    }
    const auto& status = SyncRemoteFileUtil::sync_ipdata_from_remote_path(path);
    if (!status.ok()) {
        LOG(ERROR) << "sync ipdata from remote path failed: " << status.status().detailed_message();
        return;
    }
    city = std::make_unique<ipdb::City>(status.value());
    last_update_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}


void BaseLocationService::refresh_ip_db() {
    city.reset();
}

ipdb::City* BaseLocationService::get_db() const {
    return city.get();
}

bool BaseLocationService::need_update_db() {
    if (city == nullptr) {
        return true;
    }
    uint64_t current_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return current_time - last_update_time > refresh_time;
}
}

