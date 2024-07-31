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

package com.starrocks.planner;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.StarrocksTable;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStarrocksScanNode;
import com.starrocks.thrift.TStarrocksScanRange;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StarrocksScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(StarrocksScanNode.class);

    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    public OkHttpClient networkClient = new OkHttpClient.Builder()
            .readTimeout(100, TimeUnit.SECONDS)
            .build();
    private static final String PATH_URI = "/_query_plan";

    private String queryPlan;
    private final StarrocksTable starrocksTable;
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    public StarrocksScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.starrocksTable = (StarrocksTable) desc.getTable();
    }

    public void setupScanRangeLocations(TupleDescriptor tupleDescriptor, ScalarOperator predicate) {
        List<String> fieldNames =
                tupleDescriptor.getSlots().stream().map(s -> s.getColumn().getName()).collect(Collectors.toList());
        StringBuilder sql = new StringBuilder("select " + String.join(",", fieldNames) + " from " +
                starrocksTable.getDbName() + "." + starrocksTable.getName());
        sql.append(predicate == null ? ";" : " where " + predicate.toString() + ";");
        RequestBody body =
                RequestBody.create(JSON, "{ \"sql\" :  \" " + sql.toString() + " \" }");
        String rootAuth = Credentials.basic("root", "");
        String url = "http://" + starrocksTable.getRemoteFeHost() + ":" + starrocksTable.getRemoteFePort() + "/api/" + starrocksTable.getDbName() +
                "/" + starrocksTable.getName() + PATH_URI;
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(url)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            if (response.code() != 200) {
                throw new StarRocksConnectorException("Can not submit sql to remote cluster.");
            }
            String respStr = Objects.requireNonNull(response.body()).string();
            JSONObject jsonObject = new JSONObject(respStr);

            JSONObject partitionsObject = jsonObject.getJSONObject("partitions");
            List<Long> nodeIds = getAllAvailableBackendOrComputeIds();
            int index = 0;
            for (String tabletKey : partitionsObject.keySet()) {
                JSONObject tabletObject = partitionsObject.getJSONObject(tabletKey);
                if (tabletObject.getJSONArray("routings").length() != 0) {
                    String[] tmp = tabletObject.getJSONArray("routings").getString(0).split(":");
                    queryPlan = jsonObject.getString("opaqued_query_plan");

                    TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
                    TScanRange scanRange = new TScanRange();
                    TStarrocksScanRange tStarrocksScanRange = new TStarrocksScanRange();
                    tStarrocksScanRange.setRemote_be_host(tmp[0]);
                    tStarrocksScanRange.setRemote_be_port(tmp[1]);
                    List<Long> tablet_id = new ArrayList<>();
                    tablet_id.add(Long.parseLong(tabletKey));
                    tStarrocksScanRange.setTablet_ids(tablet_id);
                    scanRange.setStarrocks_scan_range(tStarrocksScanRange);
                    scanRangeLocations.setScan_range(scanRange);

                    TScanRangeLocation scanRangeLocation = new TScanRangeLocation();
                    scanRangeLocation.setBackend_id(nodeIds.get(index++ % nodeIds.size()));
                    scanRangeLocations.addToLocations(scanRangeLocation);
                    scanRangeLocationsList.add(scanRangeLocations);
                }
            }
        } catch (Exception e) {
            LOG.error("Exception while submit read task to remote starrocks cluster:" + e.getMessage());
        }
    }

    private List<Long> getAllAvailableBackendOrComputeIds() {
        SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<Long> allNodes = infoService.getAvailableBackendIds();
        allNodes.addAll(infoService.getAvailableComputeNodeIds());
        return allNodes;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.STARROCKS_SCAN_NODE;
        TStarrocksScanNode tStarrocksScanNode = new TStarrocksScanNode();
        tStarrocksScanNode.setTuple_id(desc.getId().asInt());
        tStarrocksScanNode.setQuery_plan(queryPlan);
        tStarrocksScanNode.setDatabase(starrocksTable.getDbName());
        tStarrocksScanNode.setTable_name(starrocksTable.getName());
        tStarrocksScanNode.setUsername(starrocksTable.getRemoteFeUsername());
        tStarrocksScanNode.setPasswd(starrocksTable.getRemoteFePasswd());
        msg.starrocks_scan_node = tStarrocksScanNode;
        if (starrocksTable != null) {
            msg.starrocks_scan_node.setTable_name(starrocksTable.getName());
        }
    }
}
