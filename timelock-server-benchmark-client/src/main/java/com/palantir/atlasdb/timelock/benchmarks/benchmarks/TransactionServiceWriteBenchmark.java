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
import java.util.concurrent.atomic.AtomicLong;

import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionTables;

public final class TransactionServiceWriteBenchmark extends AbstractBenchmark {
    private final TransactionManager transactionManager;
    private final AtomicLong timestampToAssign = new AtomicLong();
    private final long numRequests;

    private TransactionServiceWriteBenchmark(TransactionManager transactionManager,
            int numClients,
            int requestsPerClient) {
        super(numClients, requestsPerClient);
        this.transactionManager = transactionManager;
        this.numRequests = numClients * requestsPerClient;
    }

    public static Map<String, Object> execute(TransactionManager transactionManager, int numClients,
            int requestsPerClient) {
        return new TransactionServiceWriteBenchmark(transactionManager, numClients, requestsPerClient).execute();
    }

    @Override
    protected void setup() {
        TransactionTables.truncateTables(transactionManager.getKeyValueService());
        long initialTimestamp = transactionManager.getTimestampService().getFreshTimestamp();
        timestampToAssign.set(initialTimestamp);
        transactionManager.getTimestampManagementService().fastForwardTimestamp(
                initialTimestamp + (numRequests * TransactionConstants.V2_TRANSACTION_NUM_PARTITIONS));
    }

    @Override
    protected void performOneCall() {
        long timestamp = timestampToAssign.getAndAdd(TransactionConstants.V2_TRANSACTION_NUM_PARTITIONS);
        transactionManager.getTransactionService().putUnlessExists(timestamp, timestamp + 1);
    }

    @Override
    protected void cleanup() {
        TransactionTables.truncateTables(transactionManager.getKeyValueService());
    }
}
