/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.http;

import java.util.List;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.api.config.service.UserAgent;

public final class TimelockUtils {
    private static final int PORT = 8080;
    private static final String NAMESPACE = "test";

    private TimelockUtils() {
    }

    public static <T> T createClient(MetricsManager metricsManager, List<String> hosts, Class<T> type) {
        List<String> endpointUris = hostnamesToEndpointUris(hosts);
        return createFromUris(metricsManager, endpointUris, type);
    }

    private static List<String> hostnamesToEndpointUris(List<String> hosts) {
        return Lists.transform(hosts, host -> String.format("http://%s:%d/%s", host, PORT, NAMESPACE));
    }

    private static <T> T createFromUris(MetricsManager metricsManager, List<String> endpointUris, Class<T> type) {
        return AtlasDbHttpClients.createProxyWithQuickFailoverForTesting(
                metricsManager,
                ImmutableServerListConfig.builder().addAllServers(endpointUris).build(),
                type,
                AuxiliaryRemotingParameters.builder()
                        .shouldRetry(true)
                        .shouldLimitPayload(false)
                        .userAgent(UserAgent.of(UserAgent.Agent.of("atlasdb-jepsen", UserAgent.Agent.DEFAULT_VERSION)))
                        .build());
    }
}
