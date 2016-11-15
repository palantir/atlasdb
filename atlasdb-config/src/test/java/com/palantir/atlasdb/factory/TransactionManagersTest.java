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
package com.palantir.atlasdb.factory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import org.junit.Test;

import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;

public class TransactionManagersTest {
    private static final String CLIENT = "foo";
    private static final String SERVER_1 = "http://localhost:8080";
    private static final String SERVER_2 = "http://palantir.com:8080";

    private static final TimeLockClientConfig CLIENT_CONFIG = ImmutableTimeLockClientConfig.builder()
            .client(CLIENT)
            .serverListConfig(ImmutableServerListConfig.builder()
                    .addServers(SERVER_1, SERVER_2)
                    .build())
            .build();

    @Test
    public void canGetNamespacedConfigsFromTimelockBlock() {
        ServerListConfig namespacedConfig = TransactionManagers.getNamespacedConfig(CLIENT_CONFIG);
        assertThat(namespacedConfig.servers(), contains(SERVER_1 + "/" + CLIENT, SERVER_2 + "/" + CLIENT));
    }
}
