/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.http;

import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jayway.awaitility.Awaitility;
import com.palantir.timestamp.TimestampService;

public final class TimestampClient {
    private static final int PORT = 8080;
    private static final String NAMESPACE = "test";
    private static final int TIMEOUT_SECONDS = 60;

    private TimestampClient() {
    }

    public static TimestampService create(List<String> hosts) {
        List<String> endpointUris = hostnamesToEndpointUris(hosts);
        return createFromUris(endpointUris);
    }

    public static void waitUntilHostReady(String host) {
        Awaitility.await()
                .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .until(() -> hostIsListening(host));
    }

    public static void waitUntilTimestampClusterReady(List<String> hosts) {
        Awaitility.await()
                .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .until(() -> clusterReturnsTimestamp(hosts));
    }

    private static List<String> hostnamesToEndpointUris(List<String> hosts) {
        return Lists.transform(hosts, host -> String.format("http://%s:%d/%s", host, PORT, NAMESPACE));
    }

    private static TimestampService createFromUris(List<String> endpointUris) {
        return AtlasDbHttpClients.createProxyWithQuickFailoverForTesting(
                Optional.<SSLSocketFactory>absent(),
                endpointUris,
                TimestampService.class);
    }

    private static boolean hostIsListening(String host) {
        try {
            new Socket(host, PORT);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean clusterReturnsTimestamp(List<String> hosts) {
        try {
            TimestampService service = create(hosts);
            service.getFreshTimestamp();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
