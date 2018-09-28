/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.qos.FakeQosClient;
import com.palantir.atlasdb.util.MetricsManagers;

public class CassandraKeyValueServiceAsyncInitializationTest {

    @Test
    public void cassandraKvsInitializesAsynchronously() {
        KeyValueService asyncInitializedKvs = CassandraKeyValueServiceImpl.create(
                MetricsManagers.createForTests(),
                CassandraContainer.KVS_CONFIG,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                CassandraContainer.LEADER_CONFIG,
                CassandraMutationTimestampProviders.legacyModeForTestsOnly(),
                true,
                FakeQosClient.INSTANCE);

        assertThat(asyncInitializedKvs.isInitialized()).isFalse();

        startCassandra();

        Awaitility.await().atMost(25, TimeUnit.SECONDS).until(asyncInitializedKvs::isInitialized);
    }

    private void startCassandra() {
        try {
            Containers containers = new Containers(CassandraKeyValueServiceAsyncInitializationTest.class)
                    .with(new CassandraContainer());
            containers.before();
        } catch (Throwable th) {
            fail("Could not start docker", th);
        }
    }
}
