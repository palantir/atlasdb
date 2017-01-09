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

public final class TimelockUtils {
    private static final int PORT = 8080;
    private static final int TIMEOUT_SECONDS = 60;
    private static final String NAMESPACE = "test";

    private TimelockUtils() {
    }

    public static void waitUntilHostReady(String host) {
        Awaitility.await()
                .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .until(() -> hostIsListening(host));
    }

    public static <T> T createClient(List<String> hosts, Class<T> type) {
        List<String> endpointUris = hostnamesToEndpointUris(hosts);
        return createFromUris(endpointUris, type);
    }

    private static List<String> hostnamesToEndpointUris(List<String> hosts) {
        return Lists.transform(hosts, host -> String.format("http://%s:%d/%s", host, PORT, NAMESPACE));
    }

    private static <T> T createFromUris(List<String> endpointUris, Class<T> type) {
        return AtlasDbHttpClients.createProxyWithQuickFailoverForTesting(
                Optional.<SSLSocketFactory>absent(),
                endpointUris,
                type);
    }

    private static boolean hostIsListening(String host) {
        try {
            new Socket(host, PORT);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
