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
package com.palantir.atlasdb.cleaner;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.common.time.Clock;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PuncherTest {
    private static final long GRANULARITY_MILLIS = 10;

    public static List<Arguments> getParameters() {
        InMemoryKeyValueService kvsPuncherStoreKvs = new InMemoryKeyValueService(false);
        InMemoryKeyValueService cachingKvsPuncherStoreKvs = new InMemoryKeyValueService(false);

        InMemoryPuncherStore inMemoryPuncherStore = InMemoryPuncherStore.create();
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(kvsPuncherStoreKvs);
        CachingPuncherStore cachingInMemoryPuncherStore =
                CachingPuncherStore.create(InMemoryPuncherStore.create(), GRANULARITY_MILLIS);
        CachingPuncherStore cachingKeyValueServicePuncherStore = CachingPuncherStore.create(
                KeyValueServicePuncherStore.create(cachingKvsPuncherStoreKvs), GRANULARITY_MILLIS);
        List<Arguments> parameters = List.of(
                Arguments.of(inMemoryPuncherStore, null),
                Arguments.of(puncherStore, kvsPuncherStoreKvs),
                Arguments.of(cachingInMemoryPuncherStore, null),
                Arguments.of(cachingKeyValueServicePuncherStore, cachingKvsPuncherStoreKvs));
        return ImmutableList.copyOf(parameters);
    }

    long timeMillis = 0;

    private final Clock clock = () -> timeMillis;

    final long firstPunchTimestamp = 33L;
    final long secondPunchTimestamp = 35L;
    final long thirdPunchTimestamp = 37L;

    final long firstTimestampToGetMillis = 34L;
    final long secondTimestampToGetMillis = 35L;
    final long thirdTimestampToGetMillis = 36L;

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(PuncherStore puncherStore, KeyValueService kvs) {
        Puncher puncher = SimplePuncher.create(puncherStore, clock, Suppliers.ofInstance(10000L));
        Supplier<Long> timestampSupplier = puncher.getTimestampSupplier();

        timeMillis += 60000L;
        assertThat((long) timestampSupplier.get()).isEqualTo(Long.MIN_VALUE);
        timeMillis += 60000L;
        assertThat((long) timestampSupplier.get()).isEqualTo(Long.MIN_VALUE);
        timeMillis += 60000L;

        final long firstExpectedMillis = timeMillis;
        puncher.punch(firstPunchTimestamp);

        timeMillis += 60000L;
        assertThat((long) timestampSupplier.get()).isEqualTo(firstPunchTimestamp);

        final long secondExpectedMillis = timeMillis;
        puncher.punch(secondPunchTimestamp);

        assertThat((long) timestampSupplier.get()).isEqualTo(firstPunchTimestamp);
        timeMillis += 60000L;
        assertThat((long) timestampSupplier.get()).isEqualTo(secondPunchTimestamp);
        timeMillis += 10L;
        assertThat((long) timestampSupplier.get()).isEqualTo(secondPunchTimestamp);

        puncher.punch(thirdPunchTimestamp);

        assertThat((long) timestampSupplier.get()).isEqualTo(secondPunchTimestamp);
        timeMillis += 60000L;
        assertThat((long) timestampSupplier.get()).isEqualTo(thirdPunchTimestamp);

        assertThat(puncherStore.getMillisForTimestamp(firstTimestampToGetMillis))
                .isEqualTo(firstExpectedMillis);
        assertThat(puncherStore.getMillisForTimestamp(secondTimestampToGetMillis))
                .isEqualTo(secondExpectedMillis);
        assertThat(puncherStore.getMillisForTimestamp(thirdTimestampToGetMillis))
                .isEqualTo(secondExpectedMillis);

        shutdownKvs(kvs);
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testBigTimestamp(PuncherStore puncherStore, KeyValueService kvs) {
        Puncher puncher = SimplePuncher.create(puncherStore, clock, Suppliers.ofInstance(10000L));
        timeMillis = (1L << 60) - 3;
        puncher.punch(1L << 62);
        shutdownKvs(kvs);
    }

    public void shutdownKvs(KeyValueService kvs) {
        if (kvs != null) {
            kvs.close();
        }
    }
}
