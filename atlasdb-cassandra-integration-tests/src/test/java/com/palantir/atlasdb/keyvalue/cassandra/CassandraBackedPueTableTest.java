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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.atomic.AtomicTable;
import com.palantir.atlasdb.atomic.ConsensusForgettingStore;
import com.palantir.atlasdb.atomic.PueKvsConsensusForgettingStore;
import com.palantir.atlasdb.atomic.ResilientCommitTimestampAtomicTable;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.encoding.BaseProgressEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionStatusUtils;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraBackedPueTableTest {
    private final KeyValueService kvs = CASSANDRA.getDefaultKvs();
    private final ConsensusForgettingStore store =
            new PueKvsConsensusForgettingStore(kvs, TransactionConstants.TRANSACTIONS2_TABLE);
    private final AtomicTable<Long, TransactionStatus> pueTable = new ResilientCommitTimestampAtomicTable(
            store,
            new TwoPhaseEncodingStrategy(BaseProgressEncodingStrategy.INSTANCE),
            new DefaultTaggedMetricRegistry());
    private final ExecutorService writeExecutor = PTExecutors.newFixedThreadPool(1);
    private final ListeningExecutorService readExecutors =
            MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(10));

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    @Before
    public void setup() {
        kvs.createTable(
                TransactionConstants.TRANSACTIONS2_TABLE,
                TableMetadata.allDefault().persistToBytes());
    }

    @After
    public void cleanup() {
        writeExecutor.shutdownNow();
        readExecutors.shutdownNow();
    }

    @Test
    public void getDoesNotLeakExceptionsInRaceConditions() throws ExecutionException, InterruptedException {
        List<Long> timestamps = LongStream.range(0, 500).mapToObj(x -> x * 100).collect(Collectors.toList());
        Iterable<List<Long>> partitionedStartTimestamps = Lists.partition(timestamps, 20);
        for (List<Long> singlePartition : partitionedStartTimestamps) {
            singlePartition.forEach(timestamp -> writeExecutor.execute(
                    () -> pueTable.update(timestamp, TransactionStatusUtils.fromTimestamp(timestamp))));

            List<ListenableFuture<Map<Long, TransactionStatus>>> reads = new ArrayList<>();
            for (int i = 0; i < singlePartition.size(); i++) {
                reads.add(Futures.submitAsync(() -> pueTable.get(singlePartition), readExecutors));
            }
            Futures.allAsList(reads).get().forEach(singleResult -> {
                for (long ts : singlePartition) {
                    assertCommitTimestampAbsentOrEqualToStartTimestamp(singleResult, ts);
                }
            });
        }
    }

    private static void assertCommitTimestampAbsentOrEqualToStartTimestamp(
            Map<Long, TransactionStatus> result, long ts) {
        Optional.ofNullable(result.get(ts))
                .flatMap(TransactionStatuses::getCommitTimestamp)
                .ifPresent(commitTs -> assertThat(ts).isEqualTo(commitTs));
    }
}
