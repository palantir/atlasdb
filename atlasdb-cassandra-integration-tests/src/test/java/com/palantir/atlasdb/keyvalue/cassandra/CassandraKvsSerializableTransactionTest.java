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

import static org.mockito.Mockito.mock;

import java.util.Optional;

import org.junit.ClassRule;

import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.SweepQueue;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;

public class CassandraKvsSerializableTransactionTest extends AbstractSerializableTransactionTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource(
            CassandraKvsSerializableTransactionTest.class);

    @Override
    protected KeyValueService getKeyValueService() {
        return CASSANDRA.getDefaultKvs();
    }

    @Override
    protected void registerTransactionManager(TransactionManager transactionManager) {
        CASSANDRA.registerTransactionManager(transactionManager);
    }

    @Override
    protected Optional<TransactionManager> getRegisteredTransactionManager() {
        return CASSANDRA.getRegisteredTransactionManager();
    }

    @Override
    protected MultiTableSweepQueueWriter getSweepQueueWriterUninitialized() {
        return TargetedSweeper.createUninitializedForTest(() -> 128);
    }

    @Override
    protected MultiTableSweepQueueWriter getSweepQueueWriterInitialized() {
        return SweepQueue.createWriter(mock(TargetedSweepMetrics.class), keyValueService, () -> 128);
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

}
