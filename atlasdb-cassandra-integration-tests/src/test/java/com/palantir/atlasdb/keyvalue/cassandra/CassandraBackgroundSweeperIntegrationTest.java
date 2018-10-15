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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Arrays;

import org.junit.ClassRule;
import org.junit.runners.Parameterized;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.AbstractBackgroundSweeperIntegrationTest;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;

public class CassandraBackgroundSweeperIntegrationTest extends AbstractBackgroundSweeperIntegrationTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraBackgroundSweeperIntegrationTest.class)
            .with(new CassandraContainer());

    @Parameterized.Parameter
    public boolean useColumnBatchSize;

    @Parameterized.Parameters(name = "Use column batch size parameter = {0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(true, false);
    }

    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    @Override
    protected KeyValueService getKeyValueService() {
        CassandraKeyValueServiceConfig config = useColumnBatchSize
                ? ImmutableCassandraKeyValueServiceConfig.copyOf(CassandraContainer.KVS_CONFIG)
                    .withTimestampsGetterBatchSize(10)
                : CassandraContainer.KVS_CONFIG;

        // Need to ensure that C* timestamps for sentinels and deletes occur after timestamps where values were put
        // (which is true in practice assuming timestamp service is working properly)
        return CassandraKeyValueServiceImpl.create(
                metricsManager,
                config,
                CassandraContainer.LEADER_CONFIG,
                CassandraTestTools.getMutationProviderWithStartingTimestamp(1_000_000));
    }
}
