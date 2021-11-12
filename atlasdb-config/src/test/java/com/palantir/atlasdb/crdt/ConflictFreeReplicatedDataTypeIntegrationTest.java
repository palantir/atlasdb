/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.crdt;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.crdt.bucket.ShotgunSeriesBucketSelector;
import com.palantir.atlasdb.crdt.generated.CrdtTable;
import com.palantir.atlasdb.crdt.generated.CrdtTableFactory;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.immutables.value.Value;
import org.junit.Test;

public class ConflictFreeReplicatedDataTypeIntegrationTest {
    private static final Series TOM = ImmutableSeries.of("tom");

    private final TransactionManager txMgr =
            TransactionManagers.createInMemory(ImmutableSet.of(CrdtSchema.INSTANCE.getLatestSchema()));

    @Test
    public void integratesStatistics() {
        List<ResourceStatistics> resourceStatistics = IntStream.range(0, 1000)
                .mapToObj(index -> ImmutableResourceStatistics.builder()
                        .numRemoteCalls(ThreadLocalRandom.current().nextLong(3L, 88L))
                        .addFilesLoaded("everywhere.txt", index + ".out")
                        .build())
                .collect(Collectors.toList());

        CountDownLatch latch = new CountDownLatch(resourceStatistics.size());
        ExecutorService executorService = Executors.newFixedThreadPool(resourceStatistics.size());
        List<Future<?>> futures = new ArrayList<>(resourceStatistics.size());
        for (ResourceStatistics resourceStatistic : resourceStatistics) {
            Future<?> future = executorService.submit(() -> {
                try {
                    txMgr.runTaskWithRetry(txn -> {
                        CrdtTable crdtTable = CrdtTableFactory.of().getCrdtTable(txn);
                        ConflictFreeReplicatedDataTypeWriter<ResourceStatistics> writer =
                                new ConflictFreeReplicatedDataTypeWriter<>(
                                        crdtTable,
                                        ResourceStatisticsAdapter.INSTANCE,
                                        new ShotgunSeriesBucketSelector(100));
                        latch.countDown();
                        latch.await();
                        writer.aggregateValue(TOM, resourceStatistic);
                        return null;
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }
        futures.forEach(Futures::getUnchecked);

        ResourceStatistics actualCombinedStats = txMgr.runTaskWithRetry(txn -> {
            CrdtTable crdtTable = CrdtTableFactory.of().getCrdtTable(txn);
            ConflictFreeReplicatedDataTypeReader<ResourceStatistics> reader =
                    new ConflictFreeReplicatedDataTypeReader<>(crdtTable, ResourceStatisticsAdapter.INSTANCE);
            return reader.read(ImmutableList.of(TOM)).get(TOM);
        });

        assertThat(actualCombinedStats.numRemoteCalls())
                .isEqualTo(resourceStatistics.stream()
                        .mapToLong(ResourceStatistics::numRemoteCalls)
                        .sum());
        assertThat(actualCombinedStats.filesLoaded())
                .isEqualTo(Sets.union(
                        ImmutableSet.of("everywhere.txt"),
                        IntStream.range(0, 1000).mapToObj(i -> i + ".out").collect(Collectors.toSet())));
    }

    private enum JacksonSerializer {
        INSTANCE;

        private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();

        <T> byte[] serializeToBytes(T object) {
            try {
                return OBJECT_MAPPER.writeValueAsBytes(object);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        <T> T deserializeFromBytes(byte[] object, Class<T> clazz) {
            try {
                return OBJECT_MAPPER.readValue(object, clazz);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private enum ResourceStatisticsAdapter implements ConflictFreeReplicatedDataTypeAdapter<ResourceStatistics> {
        INSTANCE;

        @Override
        public Function<ResourceStatistics, byte[]> serializer() {
            return JacksonSerializer.INSTANCE::serializeToBytes;
        }

        @Override
        public Function<byte[], ResourceStatistics> deserializer() {
            return bytes -> JacksonSerializer.INSTANCE.deserializeFromBytes(bytes, ResourceStatistics.class);
        }

        @Override
        public BinaryOperator<ResourceStatistics> merge() {
            return (stats1, stats2) -> ImmutableResourceStatistics.builder()
                    .numRemoteCalls(stats1.numRemoteCalls() + stats2.numRemoteCalls())
                    .addAllFilesLoaded(stats1.filesLoaded())
                    .addAllFilesLoaded(stats2.filesLoaded())
                    .build();
        }

        @Override
        public ResourceStatistics identity() {
            return ImmutableResourceStatistics.builder().numRemoteCalls(0).build();
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableResourceStatistics.class)
    @JsonDeserialize(as = ImmutableResourceStatistics.class)
    interface ResourceStatistics {
        long numRemoteCalls();

        Set<String> filesLoaded();
    }
}
