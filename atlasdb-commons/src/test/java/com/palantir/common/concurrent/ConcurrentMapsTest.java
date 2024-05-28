/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class ConcurrentMapsTest {

    private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();
    private final ListeningExecutorService executor =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(PROCESSORS));

    @AfterEach
    void after() {
        executor.shutdown();
    }

    @Test
    void canCreateMap() {
        ConcurrentMap<Integer, String> map = ConcurrentMaps.newWithExpectedEntries(256);
        assertThat(map).isInstanceOf(ConcurrentHashMap.class).isEmpty();
        assertThat(map.put(1, "1")).isNull();
        assertThat(map.put(1, "one")).isEqualTo("1");
        ConcurrentHashMap<Integer, String> chm = (ConcurrentHashMap<Integer, String>) map;
        assertThat(chm.entrySet()).hasSize(1);
        chm.put(2, "2");
        assertThat(chm).hasSize(2).containsExactlyInAnyOrderEntriesOf(Map.of(1, "one", 2, "2"));
    }

    @ParameterizedTest
    @CsvSource({
        "0, 0, 1",
        "1, 5, 8",
        "10, 59, 64",
        "16, 95, 128",
        "100, 599, 1024",
        "128, 767, 1024",
        "256, 1535, 2048",
        "1024, 6143, 8192",
    })
    void capacities(int elements, int expectedInitialCapacity, int expectedArraySize) {
        assertThat(ConcurrentMaps.initialCapacity(elements)).isEqualTo(expectedInitialCapacity);
        assertThat(ConcurrentMaps.expectedArraySize(expectedInitialCapacity)).isEqualTo(expectedArraySize);
    }

    @ParameterizedTest
    @MethodSource("maps")
    void contending(String name, int count, Map<Integer, String> map) throws Exception {
        List<ListenableFuture<String>> futures = new ArrayList<>(count);
        CountDownLatch latch = new CountDownLatch(Math.min(count, PROCESSORS));

        for (int i = 0; i < count; i++) {
            int j = i;
            futures.add(executor.submit(() -> {
                latch.countDown();
                latch.await();
                return map.put(j, Integer.toString(j));
            }));
        }
        executor.shutdown();
        assertThat(executor.awaitTermination(Duration.ofSeconds(10))).isTrue();

        assertThat(Futures.allAsList(futures).get()).hasSize(count).allSatisfy(s -> assertThat(s)
                .isNull());
        assertThat(map)
                .as("Map %s should have count entries", name)
                .hasSize(count)
                .containsExactlyInAnyOrderEntriesOf(IntStream.range(0, count)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), Object::toString)));
    }

    static Stream<Arguments> maps() {
        return IntStream.of(0, 1, 128, 256, 1024, 1_048_576)
                .boxed()
                .flatMap(i -> Stream.of(
                        Arguments.of("CHM(i)", i, new ConcurrentHashMap<>(i)),
                        Arguments.of("CM(0)", i, ConcurrentMaps.newWithExpectedEntries(0)),
                        Arguments.of("CM(i)", i, ConcurrentMaps.newWithExpectedEntries(i)),
                        Arguments.of("CHM()", i, new ConcurrentHashMap<>()),
                        Arguments.of("SHM()", i, Collections.synchronizedMap(Maps.newHashMapWithExpectedSize(i)))));
    }
}
