/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.pue.ConsensusForgettingStore;
import com.palantir.atlasdb.pue.KvsConsensusForgettingStore;
import com.palantir.atlasdb.pue.PutUnlessExistsTable;
import com.palantir.atlasdb.pue.ResilientCommitTimestampPutUnlessExistsTable;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.concurrent.PTExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraBackedPueTableTest {
    private KeyValueService kvs = CASSANDRA.getDefaultKvs();
    private ConsensusForgettingStore store =
            new KvsConsensusForgettingStore(kvs, TransactionConstants.TRANSACTIONS2_TABLE);
    private PutUnlessExistsTable<Long, Long> pueTable =
            new ResilientCommitTimestampPutUnlessExistsTable(store, TwoPhaseEncodingStrategy.INSTANCE);

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    @Before
    public void setup() {
        kvs.createTable(
                TransactionConstants.TRANSACTIONS2_TABLE,
                TableMetadata.allDefault().persistToBytes());
    }

    @Test
    public void getDoesNotLeakExceptionsInRaceConditions() throws ExecutionException, InterruptedException {
        ExecutorService writeExecutor = PTExecutors.newFixedThreadPool(1);
        ExecutorService readExecutors = PTExecutors.newFixedThreadPool(10);

        List<Long> timestamps = LongStream.range(0, 500).mapToObj(x -> x * 100).collect(Collectors.toList());
        Iterable<List<Long>> partitionedStartTimestamps = Iterables.partition(timestamps, 20);
        for (List<Long> singlePartition : partitionedStartTimestamps) {
            singlePartition.forEach(
                    timestamp -> writeExecutor.submit(() -> pueTable.putUnlessExists(timestamp, timestamp)));

            List<Future<ListenableFuture<Map<Long, Long>>>> reads = new ArrayList<>();
            for (int i = 0; i < singlePartition.size(); i++) {
                reads.add(readExecutors.submit(() -> pueTable.get(singlePartition)));
            }
            for (Future<ListenableFuture<Map<Long, Long>>> oneFuture : reads) {
                Map<Long, Long> result = oneFuture.get().get();
                for (long ts : singlePartition) {
                    assertCommitTimestampAbsentOrEqualToStartTimestamp(result, ts);
                }
            }
        }
    }

    private static void assertCommitTimestampAbsentOrEqualToStartTimestamp(Map<Long, Long> result, long ts) {
        Optional.ofNullable(result.get(ts)).ifPresent(commitTs -> assertThat(ts).isEqualTo(commitTs));
    }
}
