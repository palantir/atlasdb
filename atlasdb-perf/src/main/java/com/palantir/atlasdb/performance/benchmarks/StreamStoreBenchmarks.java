/**
 * Copyright 2016 Palantir Technologies
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

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.palantir.atlasdb.performance.benchmarks.table.StreamingTable;
import com.palantir.atlasdb.performance.schema.generated.StreamTestTableFactory;
import com.palantir.atlasdb.performance.schema.generated.ValueStreamStore;
import com.palantir.atlasdb.transaction.api.TransactionManager;

@State(Scope.Benchmark)
public class StreamStoreBenchmarks {

    @Benchmark
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 50, timeUnit = TimeUnit.SECONDS)
    public Object loadStream(StreamingTable table) {
        TransactionManager transactionManager = table.getTransactionManager();
        StreamTestTableFactory tables = StreamTestTableFactory.of();
        ValueStreamStore store = ValueStreamStore.of(transactionManager, tables);

        return transactionManager.runTaskThrowOnConflict(txn -> store.loadStream(txn, 1L));
    }
}
