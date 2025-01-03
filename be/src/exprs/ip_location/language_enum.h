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

#include <boost/algorithm/string.hpp>
#include "common/statusor.h"

namespace starrocks {
enum class LanguageEnum {
    ZH,
    EN,
    UNKNOWN
};

class LanguageEnumHelper {
public:
    static LanguageEnum get_by_name(const std::string& name) {
        auto lower_name = boost::algorithm::to_lower_copy(name);
        if (lower_name == "zh") {
            return LanguageEnum::ZH;
        } else if (lower_name == "en") {
            return LanguageEnum::EN;
        } else {
            return LanguageEnum::UNKNOWN;
        }
    }

    static bool contains(const std::string& name) {
        auto lower_name = boost::algorithm::to_lower_copy(name);
        if (lower_name == "zh" || lower_name == "en") {
            return true;
        } else {
            return false;
        }
    }

    static std::string get_name(LanguageEnum language_enum) {
        switch (language_enum) {
            case LanguageEnum::ZH:
                return "zh";
            case LanguageEnum::EN:
                return "en";
            default:
                return "unknown";
        }
    }

    static std::string get_all_names() {
        std::ostringstream oss;
        int begin = static_cast<int>(LanguageEnum::ZH);
        int end = static_cast<int>(LanguageEnum::UNKNOWN);
        for (int i = begin; i < end; i++) {
            LanguageEnum language = static_cast<LanguageEnum>(i);
            oss << get_name(language);
            if (i != end - 1) {
                oss << ", ";
            }
        }
        return oss.str();
    }
};
}