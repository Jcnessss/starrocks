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

#include <arpa/inet.h>

#include "exprs/ip_location/ip_location_manager.h"
#include "exprs/ip_location/ip360_cn_location_service.h"
#include "exprs/ip_location/ip360_en_location_service.h"

namespace starrocks {
void IpRouterService::register_router(const std::shared_ptr<IpLocation>& ip_location, const std::vector<LanguageEnum>& languages) {
    for (LanguageEnum language : languages) {
        router[language] = ip_location;
    }
}

std::shared_ptr<IpLocation> IpRouterService::route(LanguageEnum language) {
    return router[language];
}


IpLocationManager::IpLocationManager() {
    ip_router_service = std::make_unique<IpRouterService>();
    // 初始化，并注册ip解析服务
    auto chinese_location_service = std::make_shared<Ip360ChineseLocationService>();
    auto english_location_service = std::make_shared<Ip360EnglishLocationService>();
    ip_router_service->register_router(chinese_location_service, chinese_location_service->get_support_language());
    ip_router_service->register_router(english_location_service, english_location_service->get_support_language());
}

StatusOr<TaIpLocationDo> IpLocationManager::get_ip_location(std::string_view ip, LanguageEnum language) {
    IpType ip_type = get_ip_type(ip);
    if (ip_type == IpType::ERROR) {
        TaIpLocationDo ip_location_do;
        ip_location_do.ip = std::string(ip);
        if (language == LanguageEnum::ZH) {
            // 这里主要是为了兼容历史版本，防止用户根据解析的结果名称 "未知"， 做一些业务的处理 locationDo.setCountry("未知");
            ip_location_do.province = "未知";
            ip_location_do.city = "未知";
            ip_location_do.isp = "未知";
        }
        return ip_location_do;
    }
    const auto& ip_location = ip_router_service->route(language);
    if (ip_location == nullptr) {
        TaIpLocationDo ip_location_do;
        ip_location_do.ip = std::string(ip);
        return ip_location_do;
    }
    auto status = ip_location->get_ip_location(ip, language);
    if (status.ok()) {
        if (ip_type == IpType::V6 && boost::algorithm::to_lower_copy(std::string(ip)).starts_with("::ffff:") && status.value().country.empty()) {
            // ipv6 转 ipv4
            return ip_location->get_ip_location(ip.substr(7), language);
        }
    }
    return status;
}

int IpLocationManager::refresh_ip_db(LanguageEnum language) {
    const auto& base_location_service = static_pointer_cast<BaseLocationService>(ip_router_service->route(language));
    base_location_service->refresh_ip_db();
    return 1;
}

int IpLocationManager::refresh_all_ip_db() {
    for (int i = static_cast<int>(LanguageEnum::ZH); i < static_cast<int>(LanguageEnum::UNKNOWN); i++) {
        LanguageEnum language = static_cast<LanguageEnum>(i);
        refresh_ip_db(language);
    }
    return 1;
}

inline bool is_ipv4(std::string_view ip) {
    struct sockaddr_in sa;
    return inet_pton(AF_INET, ip.data(), &(sa.sin_addr)) == 1;
}

inline bool is_ipv6(std::string_view ip) {
    struct sockaddr_in6 sa6;
    return inet_pton(AF_INET6, ip.data(), &(sa6.sin6_addr)) == 1;
}

IpLocationManager::IpType IpLocationManager::get_ip_type(std::string_view ip) {
    if (is_ipv4(ip)) {
        return IpType::V4;
    }
    if (is_ipv6(ip)) {
        return IpType::V6;
    }
    return IpType::ERROR;
}
} // starrocks