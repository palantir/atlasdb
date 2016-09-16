/**
 * Copyright 2015 Palantir Technologies
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.Arrays;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Optional;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractSweeperTest;

@RunWith(Parameterized.class)
public class CassandraKeyValueServiceSweeperIntegrationTest extends AbstractSweeperTest {

    @ClassRule
    public static CassandraResources cassandraResources= CassandraResources.getCassandraResource();

    @Parameterized.Parameter
    public boolean useColumnBatchSize;

    @Parameterized.Parameters(name = "Use column batch size parameter = {0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(true, false);
    }


    @Override
    protected KeyValueService getKeyValueService() {
        ImmutableCassandraKeyValueServiceConfig config = useColumnBatchSize
                ? cassandraResources.CASSANDRA_KVS_CONFIG.withTimestampsGetterBatchSize(Optional.of(10))
                : cassandraResources.CASSANDRA_KVS_CONFIG;

        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(config), cassandraResources.LEADER_CONFIG);
    }

    @Test
    public void should_not_oom_when_there_are_many_large_values_to_sweep() {
        Assume.assumeTrue("should_not_oom test will always fail if column batch size is not set!", useColumnBatchSize);

        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        long numInsertions = 100;
        insertMultipleValues(numInsertions);

        long sweepTimestamp = numInsertions + 1;
        SweepResults sweepResults = completeSweep(sweepTimestamp);

        assertThat(sweepResults.getCellsDeleted(), equalTo(numInsertions - 1));
    }

    @Test
    public void should_return_values_for_multiple_columns_when_sweeping() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        for (int ts = 10; ts <= 150; ts += 10) {
            put("row", "col1", "value", ts);
            put("row", "col2", "value", ts + 5);
        }

        SweepResults sweepResults = completeSweep(350);

        assertThat(sweepResults.getCellsDeleted(), equalTo(28L));
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
