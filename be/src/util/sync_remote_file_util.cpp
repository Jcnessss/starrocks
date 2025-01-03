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

#include "util/sync_remote_file_util.h"
#include <string>
#include <sys/stat.h>
#include <filesystem>
#include <fmt/compile.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fstream>

namespace starrocks {
inline Status ReturnErrorAndCloseSocket(int sock, const std::string& message) {
    if (sock != -1) {
        close(sock);
    }
    return Status::InternalError(message);
}

inline Status ReturnError(const std::string& message) {
    return Status::InternalError(message);
}

inline std::string format_error(const std::string& context, int error_code) {
    return fmt::format("{}: {}", context, strerror(error_code));
}

StatusOr<std::string> SyncRemoteFileUtil::sync_ipdata_from_remote_path(std::string_view path) {
    std::string ip_data_dir = "/tmp/ip_data_ta/";
    int status = mkdir(ip_data_dir.c_str(), 0755);
    if (status != 0 && errno != EEXIST) {
        return ReturnError(format_error("Failed to create IP data directory", errno));
    }

    std::string file_name = std::filesystem::path(path).filename().string();
    std::string ip_data_file = ip_data_dir + file_name;

    struct addrinfo hints{};
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;        // IPv4
    hints.ai_socktype = SOCK_STREAM; // TCP

    Addrinfo addrinfo;
    status = getaddrinfo("ta1", "19038", &hints, addrinfo.get_mutable());
    if (status != 0) {
        return ReturnError(fmt::format("getaddrinfo failed: {}", gai_strerror(status)));
    }

    Socket sock;
    struct addrinfo* rp;
    for (rp = addrinfo.get(); rp != nullptr; rp = rp->ai_next) {
        sock.reset(socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol));
        if (sock.get() == -1) {
            continue; // 继续尝试下一个地址
        }

        timeval timeout{};
        timeout.tv_sec = 60;
        timeout.tv_usec = 0;
        setsockopt(sock.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        if (connect(sock.get(), rp->ai_addr, rp->ai_addrlen) != -1) {
            break; // 连接成功
        }
    }

    if (rp == nullptr) {
        return ReturnError("Failed to connect to ta1:19038");
    }

    std::string request = "get ";
    request.append(path);
    status = send(sock.get(), request.c_str(), request.length(), 0);
    if (status == -1) {
        return ReturnErrorAndCloseSocket(sock.get(), format_error("Failed to send request", errno));
    }

    std::ofstream outFile(ip_data_file, std::ios::binary);
    if (!outFile) {
        return ReturnErrorAndCloseSocket(sock.get(), fmt::format("Failed to open file {}", ip_data_file));
    }

    char buffer[65536];
    int bytes_read;
    while ((bytes_read = recv(sock.get(), buffer, sizeof(buffer), 0)) > 0) {
        outFile.write(buffer, bytes_read);
        if (!outFile) {
            return ReturnErrorAndCloseSocket(sock.get(), fmt::format("Failed to write to file {}", ip_data_file));
        }
    }

    if (bytes_read < 0) {
        return ReturnErrorAndCloseSocket(sock.get(), format_error("Failed to receive data", errno));
    }

    outFile.close();
    return ip_data_file;
}
}
