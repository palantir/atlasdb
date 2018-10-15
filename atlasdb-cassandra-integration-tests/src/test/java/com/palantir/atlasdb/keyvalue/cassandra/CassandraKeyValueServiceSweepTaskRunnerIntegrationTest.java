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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractSweepTaskRunnerTest;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.flake.ShouldRetry;

@RunWith(Parameterized.class)
@ShouldRetry // Some tests can fail with "could not stop heartbeat" - see also HeartbeatServiceIntegrationTest.
public class CassandraKeyValueServiceSweepTaskRunnerIntegrationTest extends AbstractSweepTaskRunnerTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(
                CassandraKeyValueServiceSweepTaskRunnerIntegrationTest.class)
            .with(new CassandraContainer());

    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    @Rule
    public final RuleChain ruleChain = SchemaMutationLockReleasingRule.createChainedReleaseAndRetry(
            getKeyValueService(), CassandraContainer.KVS_CONFIG);

    @Parameterized.Parameter
    public boolean useColumnBatchSize;

    @Parameterized.Parameters(name = "Use column batch size parameter = {0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(true, false);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        CassandraKeyValueServiceConfig config = useColumnBatchSize
                ? ImmutableCassandraKeyValueServiceConfig.copyOf(CassandraContainer.KVS_CONFIG)
                        .withTimestampsGetterBatchSize(10)
                : CassandraContainer.KVS_CONFIG;

        // Timestamp of 1,000,000 is done to ensure that tombstones are written at a Cassandra timestamp that is
        // greater than the Atlas timestamp for any values written during the test.
        return CassandraKeyValueServiceImpl.create(
                metricsManager,
                config,
                CassandraContainer.LEADER_CONFIG,
                CassandraTestTools.getMutationProviderWithStartingTimestamp(1_000_000));
    }

    @Test
    public void should_not_oom_when_there_are_many_large_values_to_sweep() {
        Assume.assumeTrue("should_not_oom test will always fail if column batch size is not set!", useColumnBatchSize);

        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        long numInsertions = 100;
        insertMultipleValues(numInsertions);

        long sweepTimestamp = numInsertions + 1;
        SweepResults results = completeSweep(sweepTimestamp).get();
        Assert.assertEquals(numInsertions - 1, results.getStaleValuesDeleted());
    }

    @Test
    public void should_return_values_for_multiple_columns_when_sweeping() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        for (int ts = 10; ts <= 150; ts += 10) {
            put("row", "col1", "value", ts);
            put("row", "col2", "value", ts + 5);
        }

        SweepResults results = completeSweep(350).get();
        Assert.assertEquals(28, results.getStaleValuesDeleted());
    }

    private void insertMultipleValues(long numInsertions) {
        for (int ts = 1; ts <= numInsertions; ts++) {
            System.out.println("putting with ts = " + ts);
            putIntoDefaultColumn("row", makeLongRandomString(), ts);
        }
    }

    private String makeLongRandomString() {
        return RandomStringUtils.random(1_000_000);
    }
}
