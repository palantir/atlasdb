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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
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
    @MethodSource("maps")
    void contending(String name, int count, ConcurrentMap<Integer, String> map) throws Exception {
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

        assertThat(Futures.allAsList(futures).get()).hasSize(count).allSatisfy(s -> assertThat(s)
                .isNull());
        assertThat(map).as("Map %s should have count entries", name).hasSize(count);
    }

    static Stream<Arguments> maps() {
        return IntStream.of(0, 1, 128, 256, 1024, 1_048_576)
                .boxed()
                .flatMap(i -> Stream.of(
                        Arguments.of("CHM(i)", i, new ConcurrentHashMap<>(i)),
                        Arguments.of("CHM(i,.75," + PROCESSORS + ")", i, new ConcurrentHashMap<>(i, 0.75f, PROCESSORS)),
                        Arguments.of("CM(0)", i, ConcurrentMaps.newWithExpectedEntries(0)),
                        Arguments.of("CM(i / 8)", i, ConcurrentMaps.newWithExpectedEntries(i / 8)),
                        Arguments.of("CM(i / 4)", i, ConcurrentMaps.newWithExpectedEntries(i / 4)),
                        Arguments.of("CM(i / 2)", i, ConcurrentMaps.newWithExpectedEntries(i / 2)),
                        Arguments.of("CM(i * .75)", i, ConcurrentMaps.newWithExpectedEntries(i * 3 / 4)),
                        Arguments.of("CM(i * 4 / 3)", i, ConcurrentMaps.newWithExpectedEntries(i * 4 / 3)),
                        Arguments.of("CM(1 + i * 4/3)", i, ConcurrentMaps.newWithExpectedEntries(1 + i * 4 / 3)),
                        Arguments.of("CM(i)", i, ConcurrentMaps.newWithExpectedEntries(i)),
                        Arguments.of("CHM()", i, new ConcurrentHashMap<>())));
    }
}
