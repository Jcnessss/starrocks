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

#include "exprs/ip_location/ip360_cn_location_service.h"

namespace starrocks {

void Ip360ChineseLocationService::remove_suffix(std::string& str) {
    // 检查并移除末尾的"省"或"市"
    for (std::string_view suffix : suffixes) {
        if (str.size() >= suffix.size() &&
            str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0) {
            str.resize(str.size() - suffix.size());
            break;
        }
    }
}

StatusOr<TaIpLocationDo> Ip360ChineseLocationService::get_ip_location(std::string_view ip, LanguageEnum language) {
    TaIpLocationDo location_do;
    location_do.ip = std::string(ip);
    load_ip_db(path);
    auto* city_db = get_db();
    if (city_db == nullptr) {
        return location_do;
    }
    try {
        auto results = city_db->Find(location_do.ip, "CN");
        std::string country_code = results.size() > 0 ? (results[0].empty()? "" : std::move(results[0])) : "";
        std::string country = results.size() > 1 ? (results[1].empty()? "" : std::move(results[1])) : "";
        std::string province = results.size() > 2 ? (results[2].empty()? "" : std::move(results[2])) : "";
        std::string city = results.size() > 3 ? (results[3].empty()? "" : std::move(results[3])) : "";
        std::string isp = results.size() > 4 ? (results[4].empty()? "" : std::move(results[4])) : "";
        std::string asn = results.size() > 5 ? (results[5].empty()? "" : std::move(results[5])) : "";

        if (country_code == "CN") {
            if (province == "中国台湾") {
                province = "台湾";
                country_code = "TW";
            } else if (province.starts_with("中国香港")) {
                province = "香港";
                country_code = "HK";
            } else if (province.starts_with("澳门")) {
                province = "澳门";
                country_code = "MO";
            } else if (province.ends_with("省") || province.ends_with("市")) {
                remove_suffix(province);
            } else if (province.starts_with("内蒙古")) {
                province = "内蒙古";
            } else if (province.starts_with("广西")) {
                province = "广西";
            } else if (province.starts_with("新疆")) {
                province = "新疆";
            } else if (province.starts_with("宁夏")) {
                province = "宁夏";
            } else if (province.starts_with("西藏")) {
                province = "西藏";
            }
            if (city.ends_with("市") && !city.starts_with("吉林")) {
                remove_suffix(city);
            }
        } else if (country_code == "B1") {
            country_code = "";
        }

        location_do.country_code = std::move(country_code);
        location_do.country = std::move(country);
        location_do.province = std::move(province);
        location_do.city = std::move(city);
        location_do.isp = std::move(isp);
        location_do.asn = std::move(asn);
        return location_do;
    } catch (const std::exception& e) {
        LOG(ERROR) << "解析IP失败: " << e.what();
        return Status::InternalError(e.what());
    }
}

std::vector<LanguageEnum> Ip360ChineseLocationService::get_support_language() {
    return {supported_languages.begin(), supported_languages.end()};
}


}