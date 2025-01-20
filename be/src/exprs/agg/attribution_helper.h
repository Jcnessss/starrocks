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

#include "exprs/agg/attribution_event_entity.h"

namespace starrocks {
struct AttributionHelper {
public:
    static void fill_events_contribution(bool relation_prop, int64_t window_minute, int64_t attribution_type, std::vector<AttributionEventEntity>& event_entity) {
        std::stable_sort(event_entity.begin(), event_entity.end(), [&](const AttributionEventEntity& e1, const AttributionEventEntity& e2) {
            if (e1.event_time != e2.event_time) {
                return e1.event_time < e2.event_time;
            } else {
                return e1.event_id < e2.event_id;
            }
        });

        int right_index = 0;
        while (right_index < event_entity.size()) {
            auto& ev = event_entity[right_index];
            if (ev.is_target_event()) { //找到目标事件
                int64_t target_event_time = ev.event_time;
                std::vector<int> indices;
                for (int i = right_index - 1; i >= 0; i--) {
                    const auto& left_ev = event_entity[i];
                    if (in_windows(left_ev.event_time, target_event_time, window_minute)) {
                        if (left_ev.is_source_event()) {
                            indices.emplace_back(i);
                        }
                    } else {
                        break;
                    }
                }
                std::stable_sort(indices.begin(), indices.end());
                if (relation_prop) {
                    relation_attribute_to_source_events(attribution_type, event_entity, indices, ev);
                } else {
                    base_attribute_to_source_events(attribution_type, event_entity, indices, ev);
                }
            }
            right_index++;
        }
    }

    static void handle_result(const std::vector<AttributionEventEntity>& event_entity, phmap::flat_hash_map<std::string, int64_t>& event_count_map,
        phmap::flat_hash_map<BaseSourceEventGroup, std::vector<SourceEventEntity>, BaseSourceEventGroupHash, BaseSourceEventGroupEqual>& event_array_map) {
        // 按eventId+sourceGroup分组后 聚合待归因事件总次数
        for (const auto& event : event_entity) {
            if (event.is_source_event() && !event_count_map.contains(event.id_source)) {
                event_count_map[event.id_source] = event.total_value;
            }
        }
        // 按照目标分组对结果进行 平铺扩展
        std::vector<SourceEventEntity> result_array;
        fetch_source_array(result_array, event_entity);
        // 按targetGroup+eventId+sourceGroup分组
        for (auto& source_event : result_array) {
            event_array_map[source_event.base_source_event_group].emplace_back(source_event);
        }
    }

    static void fetch_target_array(std::vector<SourceEventEntity>& target_array, const std::vector<AttributionEventEntity>& event_entity) {
        size_t total_size = 0;
        for (const auto& ev : event_entity) {
            if (ev.is_target_event() && ev.valid) {
                total_size++;
            }
        }
        target_array.reserve(total_size);
        for (const auto& ev : event_entity) {
            if (ev.is_target_event() && ev.valid) {
                target_array.emplace_back(ev.copy_target());
            }
        }
    }

private:
    static bool in_windows(int64_t source_event_time, int64_t target_event_time, int64_t window_minute) {
        if (window_minute > 0) {
            return target_event_time - source_event_time <= window_minute * 60 * 1000 * 1000;
        }
        auto trunc_day = [](int64_t unix_time) {
            int64_t second = unix_time / 1000000;
            int64_t micro = unix_time % 1000000;
            TimestampValue ts;
            ts.from_unixtime(second, micro, cctz::utc_time_zone());
            ts.trunc_to_day();
            return ts;
        };
        TimestampValue source_day = trunc_day(source_event_time);
        TimestampValue target_day = trunc_day(target_event_time);
        return source_day == target_day;
    }

    static void base_attribute_to_source_events(int64_t attribution_type, std::vector<AttributionEventEntity>& event_entity,
        const std::vector<int>& source_ev_indices, AttributionEventEntity& target_ev) {
        if (source_ev_indices.size() == 0) {
            target_ev.valid = true;
            return;
        }

        switch (attribution_type) {
            case AttributionConfig::first: {
                auto& first_ev = event_entity[source_ev_indices[0]];
                first_ev.add_contribution(target_ev.target_group, target_ev.contribution);
            } break;
            case AttributionConfig::last: {
                auto& last_ev = event_entity[source_ev_indices.back()];
                last_ev.add_contribution(target_ev.target_group, target_ev.contribution);
            } break;
            case AttributionConfig::linear: {
                double every_ev_contrib = target_ev.contribution / source_ev_indices.size();
                for (int index : source_ev_indices) {
                    auto& ev = event_entity[index];
                    ev.add_contribution(target_ev.target_group, every_ev_contrib);
                }
            } break;
            default: {
                target_ev.valid = true;
            } break;
        }
    }

    static void relation_attribute_to_source_events(int64_t attribution_type, std::vector<AttributionEventEntity>& event_entity,
        const std::vector<int>& source_ev_indices, AttributionEventEntity& target_ev) {
        int source_size = source_ev_indices.size();
        target_ev.valid = true;
        if (source_size == 0) return;
        switch (attribution_type) {
            case AttributionConfig::first: {
                for (int index : source_ev_indices) {
                    auto& first_ev = event_entity[index];
                    // 首次归因 关联属性需相等
                    if (target_ev.target_prop[first_ev.event_id] == first_ev.source_prop) {
                        target_ev.valid = false;
                        first_ev.add_contribution(target_ev.target_group, target_ev.contribution);
                        break;
                    }
                }
            } break;
            case AttributionConfig::last: {
                for (int i = source_size - 1; i >= 0; i--) {
                    auto& last_ev = event_entity[source_ev_indices[i]];
                    if (target_ev.target_prop[last_ev.event_id] == last_ev.source_prop) {
                        target_ev.valid = false;
                        last_ev.add_contribution(target_ev.target_group, target_ev.contribution);
                        break;
                    }
                }
            } break;
            case AttributionConfig::linear: {
                std::vector<int> valid_event_indices;
                for (int index : source_ev_indices) {
                    const auto& ev = event_entity[index];
                    if (target_ev.target_prop[ev.event_id] == ev.source_prop) {
                        valid_event_indices.emplace_back(index);
                    }
                }
                if (valid_event_indices.empty()) {
                    break;
                } else {
                    double every_ev_contrib = target_ev.contribution / valid_event_indices.size();
                    target_ev.valid = false;
                    for (int valid_event_index : valid_event_indices) {
                        auto& ev = event_entity[valid_event_index];
                        ev.add_contribution(target_ev.target_group, every_ev_contrib);
                    }
                }
            } break;
            default: break;
        }
    }

    static void fetch_source_array(std::vector<SourceEventEntity>& source_array, const std::vector<AttributionEventEntity>& event_entity) {
        size_t total_size = 0;
        for (const auto& ev : event_entity) {
            if (ev.is_source_event() && !ev.map.empty()) {
                total_size += ev.map.size();
            }
        }
        source_array.reserve(total_size);
        for (const auto& ev : event_entity) {
            if (ev.is_source_event() && !ev.map.empty()) {
                for (const auto& [group, contribution] : ev.map) {
                    source_array.emplace_back(ev.copy(group, contribution));
                }
            }
        }
    }
};


} // namespace starrocks
