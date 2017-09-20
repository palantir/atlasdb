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

package com.palantir.atlasdb.timelock.benchmarks;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public abstract class AbstractRangeScanBenchmark extends AbstractBenchmark {

    private final SerializableTransactionManager txnManager;

    private final byte[] data;
    private final int numRows;

    protected final String bucket;
    protected final int batchSize;

    public AbstractRangeScanBenchmark(int numClients, int requestsPerClient,
            SerializableTransactionManager txnManager, int dataSize, int numRows) {
        super(numClients, requestsPerClient);
        this.txnManager = txnManager;
        this.bucket = UUID.randomUUID().toString();
        this.data = randomDataOfLength(dataSize);
        this.numRows = numRows;
        this.batchSize = Math.max(1, 9_000_000 / dataSize);
    }

    protected byte[] randomDataOfLength(int dataSize) {
        byte[] result = new byte[dataSize];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    @Override
    public final void setup() {
        int numWritten = 0;
        AtomicLong key = new AtomicLong(0);
        while (numWritten < numRows) {
            int numToWrite = Math.min(numRows - numWritten, batchSize);
            txnManager.runTaskWithRetry(txn -> {
                Map<Long, byte[]> values = Maps.newHashMap();
                for (int i = 0l i < numToWrite; i++) {
                    values.put(key.getAndIncrement(), data);
                }

                writeValues(txn, values);
                return null;
            });
            numWritten += numToWrite;
        }
    }

    @Override
    protected final void performOneCall() {
        List<byte[]> results = txnManager.runTaskReadOnly(txn -> getRange(txn, 0L, numRows));

        Preconditions.checkState(results.size() == numRows);
        for (byte[] resultData : results) {
            Preconditions.checkState(resultData.length == data.length);
        }
    }

    protected abstract List<byte[]> getRange(Transaction txn, long startInclusive, long endExclusive);

    protected abstract void writeValues(Transaction txn, Map<Long, byte[]> valuesByKey);
}
