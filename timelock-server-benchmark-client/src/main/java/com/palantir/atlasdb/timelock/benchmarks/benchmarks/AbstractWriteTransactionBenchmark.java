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

package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.palantir.atlasdb.timelock.benchmarks.RandomBytes;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public abstract class AbstractWriteTransactionBenchmark extends AbstractBenchmark {

    private final TransactionManager txnManager;
    private final List<byte[]> allValues;

    protected final BenchmarksTableFactory tableFactory = BenchmarksTableFactory.of();

    protected AbstractWriteTransactionBenchmark(TransactionManager txnManager, int numClients, int requestsPerClient,
            int numRows, int dataSize) {
        super(numClients, requestsPerClient);

        this.txnManager = txnManager;

        this.allValues = IntStream.range(0, numRows)
                .mapToObj(i -> RandomBytes.ofLength(dataSize))
                .collect(Collectors.toList());
    }

    @Override
    public final void performOneCall() {
        txnManager.runTaskWithRetry(txn -> {
            writeValues(txn, allValues);
            return null;
        });
    }

    protected abstract void writeValues(Transaction txn, List<byte[]> values);

}
