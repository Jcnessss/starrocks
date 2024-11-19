//
// Created by Co1a on 24-11-11.
//

#include "aggregate_resolver.hpp"

namespace starrocks {
void AggregateFuncResolver::register_thinkingdata() {
        add_aggregate_mapping_variadic<TYPE_BIGINT, TYPE_ARRAY, IntArrayState>(
                "funnel_flow_array", false, AggregateFactory::MakeFunnelFlowArrayAggregateFunction());
        add_aggregate_mapping_variadic<TYPE_DATETIME,TYPE_ARRAY,FunnelPackedTimeCollectState>(
                "funnel_packed_time_collect", false, AggregateFactory::MakeFunnelPackedTimeAggregateFunction());
        add_aggregate_mapping_variadic<TYPE_DATETIME,TYPE_ARRAY,FunnelPackedTimeCollectState>(
                "funnel_packed_time_collect2", false, AggregateFactory::MakeFunnelPackedTimeAggregateFunction2());
        add_aggregate_mapping_variadic<TYPE_BIGINT,TYPE_BIGINT,BitwishAggState>(
                "bitwise_or_agg", false, AggregateFactory::MakeBitwishAggregateFunction());
        add_aggregate_mapping_variadic<TYPE_BIGINT,TYPE_MAP,FunnelFlowArrayDate>(
                "funnel_flow_array_date",false,AggregateFactory::MakeFunnelFlowArrayDateAggregateFunction());
    }
}