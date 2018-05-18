/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import org.junit.ClassRule;

import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.queue.KvsSweepQueue;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.SweepTimestampProvider;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;

public class CassandraKeyValueServiceSerializableTransactionIntegrationTest
        extends AbstractSerializableTransactionTest {
    @ClassRule
    public static final Containers CONTAINERS =
            new Containers(CassandraKeyValueServiceSerializableTransactionIntegrationTest.class)
                    .with(new CassandraContainer());

    @Override
    protected KeyValueService getKeyValueService() {
        return CassandraKeyValueServiceImpl.create(
                CassandraContainer.KVS_CONFIG,
                CassandraContainer.LEADER_CONFIG);
    }

    @Override
    protected MultiTableSweepQueueWriter getSweepQueueWriterUninitialized() {
       return KvsSweepQueue.createUninitialized(() -> true, () -> 128, 0, 0);
    }

    @Override
    protected MultiTableSweepQueueWriter getSweepQueueWriterInitialized() {
        KvsSweepQueue queue = KvsSweepQueue.createUninitialized(() -> true, () -> 128, 0, 0);
        queue.initialize(new SweepTimestampProvider(() -> 0, () -> 0), keyValueService);
        return queue;
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

}
