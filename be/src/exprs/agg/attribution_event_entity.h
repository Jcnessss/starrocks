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

#pragma once

#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "util/slice.h"


namespace starrocks {
struct AttributionConfig {
    constexpr static int target_event = 3;
    constexpr static int source_event = 1;
    constexpr static int first = 1;
    constexpr static int last = 2;
    constexpr static int linear = 3;
};

struct SliceVectorHash {
    size_t operator()(const std::vector<Slice>& vec) const {
        size_t hash = 0;
        for (const auto& slice : vec) {
            hash = hash * 131 + SliceHashWithSeed<PhmapSeed1>()(slice);
        }
        return hash;
    }
};
struct SliceVectorEqual {
    bool operator()(const std::vector<Slice>& lhs,
                          const std::vector<Slice>& rhs) const {
        if (&lhs == &rhs) return true;
        if (lhs.size() != rhs.size()) return false;
        return std::equal(lhs.begin(), lhs.end(), rhs.begin());
    }
};

struct BaseSourceEventGroup {
    int32_t event_id;
    std::vector<Slice> source_group;
    std::vector<Slice> target_group;

    std::string fetch_id_source() const {
        std::string id_source;

        size_t total_size = std::to_string(event_id).size();
        if (!source_group.empty()) {
            total_size += source_group.size() - 1;// 逗号数量
            for (const auto& str : source_group) {
                total_size += str.size;
            }
        }

        id_source.reserve(total_size);

        id_source = std::to_string(event_id);
        if (!source_group.empty()) {
            for (size_t i = 0; i < source_group.size(); ++i) {
                if (i > 0) id_source += ",";
                id_source += source_group[i];
            }
        }
        return id_source;
    }
};

struct BaseSourceEventGroupHash {
    bool operator()(const BaseSourceEventGroup& base_source_event_group) const {
        size_t hash = 0;
        hash = hash * 131 + std::hash<int32_t>()(base_source_event_group.event_id);
        hash += hash * 131 + SliceVectorHash()(base_source_event_group.source_group);
        hash += hash * 131 + SliceVectorHash()(base_source_event_group.target_group);
        return hash;
    }
};

struct BaseSourceEventGroupEqual {
    bool operator()(const BaseSourceEventGroup& lhs, const BaseSourceEventGroup& rhs) const {
        if (lhs.event_id != rhs.event_id) {
            return false;
        }

        if (lhs.source_group.size() != rhs.source_group.size()) {
            return false;
        }

        if (!std::equal(lhs.source_group.begin(), lhs.source_group.end(), rhs.source_group.begin(), rhs.source_group.end())) {
            return false;
        }

        if (lhs.target_group.size() != rhs.target_group.size()) {
            return false;
        }

        if (!std::equal(lhs.target_group.begin(), lhs.target_group.end(), rhs.target_group.begin(), rhs.target_group.end())) {
            return false;
        }

        return true;
    }
};

struct SourceEventEntity {
    double contribution;
    BaseSourceEventGroup base_source_event_group;
};

struct AttributionEventEntity {
    bool valid = false;
    int32_t event_id;
    int32_t event_type;
    int64_t event_time;
    int64_t total_value;
    double contribution;
    std::vector<Slice> source_group;
    std::vector<Slice> target_group;
    std::vector<Slice> target_prop;
    Slice source_prop;
    std::string id_source;
    phmap::flat_hash_map<std::vector<Slice>, double, SliceVectorHash, SliceVectorEqual> map;

    int32_t get_serialize_fix_size() const {
        return sizeof(int32_t) * 2 + sizeof(int64_t) * 2 + sizeof(double);
    }

    bool is_target_event() const {
        return event_type == AttributionConfig::target_event;
    }

    bool is_source_event() const {
        return event_type == AttributionConfig::source_event;
    }

    void add_contribution(const std::vector<Slice>& arr, double contribution) {
        if (map.contains(arr)) {
            map[arr] += contribution;
        } else {
            map[arr] = contribution;
        }
    }

    void set_id_info() {
        size_t total_size = std::to_string(event_id).size();
        if (!source_group.empty()) {
            total_size += source_group.size() - 1;// 逗号数量
            for (const auto& str : source_group) {
                total_size += str.size;
            }
        }

        id_source.reserve(total_size);

        id_source = std::to_string(event_id);
        if (!source_group.empty()) {
            for (size_t i = 0; i < source_group.size(); ++i) {
                if (i > 0) id_source += ",";
                id_source += source_group[i];
            }
        }
    }

    SourceEventEntity copy(const std::vector<Slice>& target_group, double conversion) const {
        SourceEventEntity result;
        BaseSourceEventGroup base;
        base.event_id = event_id;
        base.source_group = source_group;
        base.target_group = target_group;
        result.base_source_event_group = base;
        result.contribution = conversion;
        return result;
    }

    SourceEventEntity copy_target() const {
        SourceEventEntity result;
        BaseSourceEventGroup base;
        base.event_id = event_id;
        base.source_group = {};
        base.target_group = target_group;
        result.base_source_event_group = base;
        result.contribution = contribution;
        return result;
    }


};

} // namespace starrocks
