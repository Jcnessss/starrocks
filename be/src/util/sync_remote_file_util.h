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

#pragma once
#include <netdb.h>
#include "common/statusor.h"

namespace starrocks {
class Socket {
public:
    explicit Socket(int fd = -1) : _fd(fd) {}
    ~Socket() {
        if (_fd != -1) {
            close(_fd);
        }
    }

    int get() const { return _fd; }
    void reset(int fd = -1) {
        if (_fd != -1) {
            close(_fd);
        }
        _fd = fd;
    }

private:
    int _fd;
};

class Addrinfo {
public:
    explicit Addrinfo(struct addrinfo* info = nullptr) : _info(info) {}
    ~Addrinfo() {
        if (_info != nullptr) {
            freeaddrinfo(_info);
        }
    }

    struct addrinfo* get() const { return _info; }

    struct addrinfo** get_mutable() { return &_info; }

    void reset(struct addrinfo* info = nullptr) {
        if (_info != nullptr) {
            freeaddrinfo(_info);
        }
        _info = info;
    }

private:
    struct addrinfo* _info;
};

struct SyncRemoteFileUtil {
    static StatusOr<std::string> sync_ipdata_from_remote_path(std::string_view path);
};
}
