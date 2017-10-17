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

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractSweepTaskRunnerTest;

public class CassandraKeyValueServiceSweepTaskRunnerIntegrationTest extends AbstractSweepTaskRunnerTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(
                CassandraKeyValueServiceSweepTaskRunnerIntegrationTest.class)
            .with(new CassandraContainer());

    @Override
    protected KeyValueService getKeyValueService() {
        return CassandraKeyValueServiceImpl.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(
                        CassandraContainer.KVS_CONFIG), CassandraContainer.LEADER_CONFIG);
    }

    @Test
    public void should_return_values_for_multiple_columns_when_sweeping() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        for (int ts = 10; ts <= 150; ts += 10) {
            put("row", "col1", "value", ts);
            put("row", "col2", "value", ts + 5);
        }

        SweepResults results = completeSweep(350);
        Assert.assertEquals(28, results.getStaleValuesDeleted());
    }

}
