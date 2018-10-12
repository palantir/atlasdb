/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractSweepTaskRunnerTest;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.flake.ShouldRetry;

@ShouldRetry // Some tests can fail with "could not stop heartbeat" - see also HeartbeatServiceIntegrationTest.
public class CassandraKeyValueServiceSweepTaskRunnerIntegrationTest extends AbstractSweepTaskRunnerTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource(
            CassandraKeyValueServiceSweepTaskRunnerIntegrationTest.class,
            CassandraKeyValueServiceSweepTaskRunnerIntegrationTest::createKeyValueService);

    @Rule
    public final RuleChain ruleChain = SchemaMutationLockReleasingRule.createChainedReleaseAndRetry(
            kvs, CASSANDRA.getConfig());

    public CassandraKeyValueServiceSweepTaskRunnerIntegrationTest() {
        super(CASSANDRA, CASSANDRA);
    }

    @Test
    public void should_not_oom_when_there_are_many_large_values_to_sweep() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        long numInsertions = 100;
        insertMultipleValues(numInsertions);

        long sweepTimestamp = numInsertions + 1;
        SweepResults results = completeSweep(sweepTimestamp).get();
        Assert.assertEquals(numInsertions - 1, results.getStaleValuesDeleted());
    }

    private static KeyValueService createKeyValueService() {
        return CassandraKeyValueServiceImpl.create(
                MetricsManagers.createForTests(),
                CASSANDRA.getConfig(),
                CassandraResource.LEADER_CONFIG,
                CassandraTestTools.getMutationProviderWithStartingTimestamp(1_000_000));
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
