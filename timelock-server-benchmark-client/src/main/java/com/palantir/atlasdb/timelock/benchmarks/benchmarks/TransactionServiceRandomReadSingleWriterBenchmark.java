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

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;

public final class TransactionServiceRandomReadSingleWriterBenchmark
        extends AbstractTransactionServiceRandomReadBenchmark {
    private final TransactionManager txManager;
    private final int numStartTimestamps;
    private final int permittedDrift;

    private TransactionServiceRandomReadSingleWriterBenchmark(
            TransactionManager txManager,
            int numClients,
            int requestsPerClient,
            int permittedDrift) {
        super(txManager, numClients, requestsPerClient);

        Preconditions.checkState(permittedDrift >= 0, "Gap from start to commit timestamp cannot be negative");

        this.txManager = txManager;
        this.numStartTimestamps = numClients * requestsPerClient;
        this.permittedDrift = permittedDrift;
    }

    public static Map<String, Object> execute(TransactionManager txnManager, int numClients,
            int requestsPerClient, int permittedDrift) {
        return new TransactionServiceRandomReadSingleWriterBenchmark(
                txnManager, numClients, requestsPerClient, permittedDrift)
                .execute();
    }

    @Override
    Map<Long, Long> getStartToCommitTimestampPairs() {
        Map<Long, Long> timestamps = Maps.newHashMap();
        long timestampLowerBound = txManager.getTimestampService().getFreshTimestamp();
        for (int i = 0; i < numStartTimestamps; i++) {
            // Assume partition 0
            long baseTimestamp = timestampLowerBound + i * TicketsEncodingStrategy.ROWS_PER_QUANTUM;
            timestamps.put(baseTimestamp, baseTimestamp + ThreadLocalRandom.current().nextInt(permittedDrift));
        }
        return timestamps;
    }

    @Override
    void prepareExternalDependencies() {
        txManager.getTimestampManagementService().fastForwardTimestamp(
                txManager.getTimestampService().getFreshTimestamp()
                        + numStartTimestamps * TicketsEncodingStrategy.ROWS_PER_QUANTUM);
    }
}
