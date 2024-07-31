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

package com.starrocks.connector.sr;

import com.google.common.base.Strings;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.Map;

public class StarrocksConnector implements Connector {
    private final String catalogName;

    private static final String REMOTE_FRONTEND_HOST = "remote.frontend.host";
    private static final String REMOTE_FRONTEND_RPC_PORT = "remote.frontend.rpc.port";
    private static final String REMOTE_FRONTEND_HTTP_PORT = "remote.frontend.http.port";
    private static final String REMOTE_FRONTEND_USERNAME = "remote.frontend.username";
    private static final String REMOTE_FRONTEND_PASSWD = "remote.frontend.passwd";

    private final String remoteFeHost;
    private final String remoteFeRpcPort;
    private final String remoteFeHttpPort;
    private final String remoteFeUsername;
    private final String remoteFePasswd;
    private final Map<String, String> properties;
    public StarrocksConnector(ConnectorContext context) {
        this.properties = context.getProperties();
        this.catalogName = context.getCatalogName();
        remoteFeHost = getPropertyOrThrow(REMOTE_FRONTEND_HOST);
        remoteFeRpcPort = getPropertyOrThrow(REMOTE_FRONTEND_RPC_PORT);
        remoteFeHttpPort = getPropertyOrThrow(REMOTE_FRONTEND_HTTP_PORT);
        remoteFeUsername = getPropertyOrThrow(REMOTE_FRONTEND_USERNAME);
        remoteFePasswd = properties.get(REMOTE_FRONTEND_PASSWD);
    }

    private String getPropertyOrThrow(String propertyName) {
        String propertyValue = properties.get(propertyName);
        if (Strings.isNullOrEmpty(propertyValue)) {
            throw new StarRocksConnectorException("The property %s must be set.", propertyName);
        }
        return propertyValue;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new StarrocksMetadata(
                catalogName, remoteFeHost, remoteFeRpcPort, remoteFeHttpPort, remoteFeUsername, remoteFePasswd);
    }
}
