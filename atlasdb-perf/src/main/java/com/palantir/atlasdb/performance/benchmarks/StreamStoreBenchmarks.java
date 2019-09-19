/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.performance.benchmarks;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import com.palantir.atlasdb.performance.benchmarks.table.StreamingTable;
import com.palantir.atlasdb.performance.schema.generated.StreamTestTableFactory;
import com.palantir.atlasdb.performance.schema.generated.ValueStreamStore;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
public class StreamStoreBenchmarks {

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public void loadSmallStream(StreamingTable table) throws IOException {
        long id = table.getSmallStreamId();
        TransactionManager transactionManager = table.getTransactionManager();
        StreamTestTableFactory tables = StreamTestTableFactory.of();
        ValueStreamStore store = ValueStreamStore.of(transactionManager, tables);
        try (InputStream inputStream = transactionManager.runTaskThrowOnConflict(txn -> store.loadStream(txn, id));
                InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            String line = bufferedReader.readLine();

            assertThat(line, startsWith("bytes"));
        }
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 30, timeUnit = TimeUnit.SECONDS)
    public void loadLargeStream(StreamingTable table) throws IOException {
        long id = table.getLargeStreamId();
        TransactionManager transactionManager = table.getTransactionManager();
        StreamTestTableFactory tables = StreamTestTableFactory.of();
        ValueStreamStore store = ValueStreamStore.of(transactionManager, tables);
        try (InputStream inputStream = transactionManager.runTaskThrowOnConflict(txn -> store.loadStream(txn, id))) {
            byte[] firstBytes = new byte[16];
            int read = inputStream.read(firstBytes);
            assertThat(read, is(16));
            assertArrayEquals(table.getLargeStreamFirstBytes(), firstBytes);
        }
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 30, timeUnit = TimeUnit.SECONDS)
    public void loadVeryLargeStream(StreamingTable table) throws IOException {
        long id = table.getVeryLargeStreamId();
        TransactionManager transactionManager = table.getTransactionManager();
        StreamTestTableFactory tables = StreamTestTableFactory.of();
        ValueStreamStore store = ValueStreamStore.of(transactionManager, tables);
        try (InputStream inputStream = transactionManager.runTaskThrowOnConflict(txn -> store.loadStream(txn, id))) {
            byte[] firstBytes = new byte[16];
            int read = inputStream.read(firstBytes);
            assertThat(read, is(16));
            assertArrayEquals(table.getVeryLargeStreamFirstBytes(), firstBytes);
        }
    }
}
