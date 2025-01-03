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

#include "exprs/ip_location/ip_location.h"

namespace starrocks {

class IpRouterService {
public:
    void register_router(const std::shared_ptr<IpLocation>& ip_location, const std::vector<LanguageEnum>& languages);

    std::shared_ptr<IpLocation> route(LanguageEnum language);

private:
    std::unordered_map<LanguageEnum, std::shared_ptr<IpLocation>> router;
};

class IpLocationManager {
public:
    IpLocationManager();

    StatusOr<TaIpLocationDo> get_ip_location(std::string_view ip, LanguageEnum language);

    int refresh_ip_db(LanguageEnum language);

    int refresh_all_ip_db();

private:
    enum class IpType {
        V4, V6, ERROR
    };

    IpType get_ip_type(std::string_view ip);

    std::unique_ptr<IpRouterService> ip_router_service;
};

} // starrocks
