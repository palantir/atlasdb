/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.performance.benchmarks;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.performance.benchmarks.table.StreamingTable;
import com.palantir.atlasdb.performance.schema.generated.StreamTestTableFactory;
import com.palantir.atlasdb.performance.schema.generated.ValueStreamStore;
import com.palantir.atlasdb.transaction.api.TransactionManager;

@State(Scope.Benchmark)
public class StreamStorePutBenchmarks {

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 30, timeUnit = TimeUnit.SECONDS)
    public void storeStreamWithTransaction(StreamingTable streamingTable) throws IOException {
        TransactionManager transactionManager = streamingTable.getTransactionManager();
        StreamTestTableFactory tableFactory = StreamTestTableFactory.of();
        ValueStreamStore streamTestStreamStore = ValueStreamStore.of(
                transactionManager,
                tableFactory
        );

        transactionManager.runTaskThrowOnConflict(txn -> streamTestStreamStore.storeStreams(txn,
                ImmutableMap.of(txn.getTimestamp(), streamingTable.getStreamToWrite())));
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 30, timeUnit = TimeUnit.SECONDS)
    public void storeStreamWithoutTransaction(StreamingTable streamingTable) throws IOException {
        TransactionManager transactionManager = streamingTable.getTransactionManager();
        StreamTestTableFactory tableFactory = StreamTestTableFactory.of();
        ValueStreamStore streamTestStreamStore = ValueStreamStore.of(
                transactionManager,
                tableFactory
        );

        final Long streamId = streamTestStreamStore.storeStream(streamingTable.getStreamToWrite()).getLhSide();

        transactionManager.runTaskThrowOnConflict(txn -> {
            streamTestStreamStore.markStreamAsUsed(txn, streamId, streamingTable.getMarker());
            return null;
        });
    }
}
