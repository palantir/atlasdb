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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractSweepTaskRunnerTest;
import com.palantir.atlasdb.util.MetricsManagers;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CassandraKeyValueServiceSweepTaskRunnerIntegrationTest extends AbstractSweepTaskRunnerTest {
    @ClassRule
    public static final CassandraResource CASSANDRA =
            new CassandraResource(CassandraKeyValueServiceSweepTaskRunnerIntegrationTest::createKeyValueService);

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

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
        assertThat(results.getStaleValuesDeleted()).isEqualTo(numInsertions - 1);
    }

    private static KeyValueService createKeyValueService() {
        return CassandraKeyValueServiceImpl.create(
                MetricsManagers.createForTests(),
                CASSANDRA.getConfig(),
                CassandraTestTools.getMutationProviderWithStartingTimestamp(1_000_000, temporaryFolder));
    }

    private void insertMultipleValues(long numInsertions) {
        for (int ts = 1; ts <= numInsertions; ts++) {
            System.out.println("putting with ts = " + ts);
            putIntoDefaultColumn("row", makeLongRandomString(), ts);
        }
    }

    private static String makeLongRandomString() {
        return RandomStringUtils.random(1_000_000);
    }
}
