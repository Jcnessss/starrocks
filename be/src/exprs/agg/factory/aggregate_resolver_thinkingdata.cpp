//
// Created by Co1a on 24-11-11.
//

#include "aggregate_resolver.hpp"

namespace starrocks {
void AggregateFuncResolver::register_thinkingdata() {
        add_aggregate_mapping_variadic<TYPE_BIGINT, TYPE_ARRAY, IntArrayState>(
                "funnel_flow_array", false, AggregateFactory::MakeFunnelFlowArrayAggregateFunction());
    }
}