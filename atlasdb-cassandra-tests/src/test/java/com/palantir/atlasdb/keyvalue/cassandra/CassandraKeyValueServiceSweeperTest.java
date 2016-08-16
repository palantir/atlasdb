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

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cleaner.AbstractSweeperTest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

public class CassandraKeyValueServiceSweeperTest extends AbstractSweeperTest {
    @Override
    protected KeyValueService getKeyValueService() {
        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraTestSuite.CASSANDRA_KVS_CONFIG), CassandraTestSuite.LEADER_CONFIG);
    }

    @Test
    public void should_not_oom_when_there_are_many_large_values_to_sweep() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        long numInsertions = 100;
        insertMultipleValues(numInsertions);

        long sweepTimestamp = numInsertions + 1;
        SweepResults sweepResults = completeSweep(sweepTimestamp);

        assertThat(sweepResults.getCellsDeleted(), equalTo(numInsertions - 1));
    }

    private void insertMultipleValues(long numInsertions) {
        for (int ts = 1; ts <= numInsertions; ts++) {
            System.out.println("putting with ts = " + ts);
            put("row1", makeLongRandomString(), ts);
        }
    }

    private String makeLongRandomString() {
        return RandomStringUtils.random(1_000_000);
    }
}