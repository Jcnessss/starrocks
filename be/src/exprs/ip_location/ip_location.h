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

#include "common/statusor.h"
#include "exprs/ip_location/language_enum.h"

namespace starrocks {

struct TaIpLocationDo {
    std::string ip;
    std::string country;
    std::string province;
    std::string city;
    std::string country_code;
    std::string isp;
    std::string asn;
};

class IpLocation {
public:
    virtual ~IpLocation() = default;

    /**
     * 解析 IP 地址
     *
     * @param ip IP 地址
     * @param language 显示的地理位置信息语言
     * @return 地理位置信息 (TaIpLocationDo)
     */
    virtual StatusOr<TaIpLocationDo> get_ip_location(std::string_view ip, LanguageEnum language) = 0;

    /**
     * 支持的语言
     *
     * @return 支持的语言列表
     */
    virtual std::vector<LanguageEnum> get_support_language() = 0;
};
}