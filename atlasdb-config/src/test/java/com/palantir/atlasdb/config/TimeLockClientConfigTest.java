/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.config;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TimeLockClientConfigTest {
    private static final String CLIENT = "testClient";
    private static final String SERVER_1 = "http://localhost:8080";
    private static final String SERVER_2 = "http://palantir.com:8080";

    private static final TimeLockClientConfig MULTIPLE_SERVER_CONFIG
            = getTimelockConfigForServers(ImmutableList.of(SERVER_1, SERVER_2));

    @Test
    public void canGetNamespacedConfigsFromTimelockBlock() {
        ServerListConfig namespacedConfig = MULTIPLE_SERVER_CONFIG.toNamespacedServerList();
        assertThat(namespacedConfig.servers(), hasItems(SERVER_1 + "/" + CLIENT, SERVER_2 + "/" + CLIENT));
    }

    private static TimeLockClientConfig getTimelockConfigForServers(List<String> servers) {
        return ImmutableTimeLockClientConfig.builder()
                .client(CLIENT)
                .serversList(ImmutableServerListConfig.builder()
                        .addAllServers(servers)
                        .build())
                .build();
    }
}
