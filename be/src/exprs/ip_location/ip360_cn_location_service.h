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

#include <array>
#include "exprs/ip_location/base_location_service.h"

namespace starrocks {
class Ip360ChineseLocationService : public BaseLocationService {
public:
    StatusOr<TaIpLocationDo> get_ip_location(std::string_view ip, LanguageEnum language) override;

    std::vector<LanguageEnum> get_support_language() override;

private:

    static void remove_suffix(std::string& str);

    constexpr static std::array<LanguageEnum, 1> supported_languages = {LanguageEnum::ZH};

    constexpr static std::string_view path = "/data/home/ta/ip_data_ta/aw.ipdb";

    constexpr static std::string_view suffixes[] = {"省", "市"};
};
}