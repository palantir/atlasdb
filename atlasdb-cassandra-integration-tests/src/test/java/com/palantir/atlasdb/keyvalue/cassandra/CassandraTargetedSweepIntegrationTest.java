/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.AbstractSweepTest;
import com.palantir.atlasdb.sweep.queue.BackgroundSweepQueueProcessor;
import com.palantir.atlasdb.sweep.queue.SweepTimestampProvider;
import com.palantir.atlasdb.sweep.queue.test.InMemorySweepQueue;
import com.palantir.atlasdb.sweep.queue.test.InMemorySweepQueueProcessorFactory;

public class CassandraTargetedSweepIntegrationTest extends AbstractSweepTest {

    private SweepTimestampProvider timestamps = mock(SweepTimestampProvider.class);
    private BackgroundSweepQueueProcessor processor;

    @Before
    public void setup() {
        super.setup();

        InMemorySweepQueue.clear();
        InMemorySweepQueueProcessorFactory factory = new InMemorySweepQueueProcessorFactory(kvs, ssm, timestamps);
        processor = new BackgroundSweepQueueProcessor(kvs, factory::getProcessorForTable);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        CassandraKeyValueServiceConfig config = CassandraContainer.KVS_CONFIG;

        return CassandraKeyValueServiceImpl.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(config), CassandraContainer.LEADER_CONFIG);
    }

    @Override
    protected Optional<SweepResults> completeSweep(TableReference tableReference, long ts) {
        when(timestamps.getSweepTimestamp(any())).thenReturn(ts);
        processor.sweepOneBatchForAllTable();
        return Optional.empty();
    }

}
