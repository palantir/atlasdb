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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.timelock.benchmarks.RandomBytes;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public class WriteTransactionBenchmark extends AbstractBenchmark {

    private static final Logger log = LoggerFactory.getLogger(WriteTransactionBenchmark.class);
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    private final TransactionManager txnManager;
    private final BenchmarksTableFactory tableFactory = BenchmarksTableFactory.of();

    private final List<byte[]> values;

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient, int numRows, int dataSize) {
        return new WriteTransactionBenchmark(txnManager, numClients, requestsPerClient, numRows, dataSize).execute();
    }

    private WriteTransactionBenchmark(TransactionManager txnManager, int numClients, int requestsPerClient,
            int numRows, int dataSize) {
        super(numClients, requestsPerClient);

        this.txnManager = txnManager;

        this.values = IntStream.range(0, numRows)
                .mapToObj(i -> RandomBytes.ofLength(dataSize))
                .collect(Collectors.toList());
    }

    @Override
    public void performOneCall() {
        List<Future> futures = Lists.newArrayList();
        for (List<byte[]> batch : Iterables.partition(values, values.size()/16)) {
            futures.add(executor.submit(() -> {
                txnManager.runTaskWithRetry(txn -> {
                    BlobsTable table = tableFactory.getBlobsTable(txn);


                    for (byte[] value : batch) {
                        table.putData(BlobsTable.BlobsRow.of(RandomBytes.ofLength(16)), value);
                    }

                    return null;
                });
            }));
        }

        for (Future fut : futures) {
            try {
                fut.get();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

//    @Override
//    public void performOneCall() {
//        String bucket = UUID.randomUUID().toString();
//        txnManager.runTaskWithRetry(txn -> {
//            KvDynamicColumns2Table table = tableFactory.getKvDynamicColumns2Table(txn);
//
//            for (byte[] value : values) {
//                table.put(KvDynamicColumns2Table.KvDynamicColumns2Row.of(bucket), KvDynamicColumns2Table.KvDynamicColumns2ColumnValue.of(
//                        KvDynamicColumns2Table.KvDynamicColumns2Column.of(RandomBytes.ofLength(16)), value));
//            }
//            return null;
//        });
//    }

}
