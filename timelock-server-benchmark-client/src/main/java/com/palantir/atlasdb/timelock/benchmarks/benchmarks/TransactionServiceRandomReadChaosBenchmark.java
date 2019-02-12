/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionTables;

public class TransactionServiceRandomReadChaosBenchmark extends AbstractBenchmark {
    private final TransactionManager txManager;
    private final Map<Long, Long> timestampMap = Maps.newHashMap();
    private final Queue<Long> queryTimestampSource = new ConcurrentLinkedDeque<>();
    private final int numStartTimestamps;
    private final int permittedDrift;

    private TransactionServiceRandomReadChaosBenchmark(
            TransactionManager txManager,
            int numClients,
            int requestsPerClient,
            int permittedDrift) {
        super(numClients, requestsPerClient);

        Preconditions.checkState(permittedDrift >= 0, "Gap from start to commit timestamp cannot be negative");

        this.txManager = txManager;
        this.numStartTimestamps = numClients * requestsPerClient;
        this.permittedDrift = permittedDrift;
    }

    public static Map<String, Object> execute(TransactionManager txnManager, int numClients,
            int requestsPerClient, int permittedDrift) {
        return new TransactionServiceRandomReadChaosBenchmark(
                txnManager, numClients, requestsPerClient, permittedDrift)
                .execute();
    }

    @Override
    protected void setup() {
        TransactionTables.truncateTables(txManager.getKeyValueService());
        populateTransactionTable();
    }

    private void populateTransactionTable() {
        long timestampLowerBound = txManager.getTimestampService().getFreshTimestamp();
        for (int i = 0; i < numStartTimestamps; i++) {
            timestampLowerBound = timestampLowerBound + ThreadLocalRandom.current().nextLong(
                    1, TicketsEncodingStrategy.PARTITIONING_QUANTUM / 10);
            timestampMap.put(
                    timestampLowerBound,
                    timestampLowerBound + ThreadLocalRandom.current().nextInt(permittedDrift));
        }
        txManager.getTimestampManagementService().fastForwardTimestamp(timestampLowerBound);
        txManager.getTransactionService().putUnlessExistsMultiple(timestampMap);
        List<Long> baseTimestamps = new ArrayList<>(timestampMap.keySet());
        Collections.shuffle(baseTimestamps);
        queryTimestampSource.addAll(baseTimestamps);
    }

    @Override
    protected void performOneCall() {
        Long timestampToQueryFor = queryTimestampSource.poll();
        Long commitTimestamp = txManager.getTransactionService().get(timestampToQueryFor);
        assertThat(commitTimestamp).isEqualTo(timestampMap.get(timestampToQueryFor));
    }

    @Override
    protected void cleanup() {
        TransactionTables.truncateTables(txManager.getKeyValueService());
    }
}
