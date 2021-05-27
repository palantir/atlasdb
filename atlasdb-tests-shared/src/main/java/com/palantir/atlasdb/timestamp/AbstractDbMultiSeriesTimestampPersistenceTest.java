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

package com.palantir.atlasdb.timestamp;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.timestamp.TimestampBoundStore;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public abstract class AbstractDbMultiSeriesTimestampPersistenceTest {
    private static final TimestampSeries DEFAULT_SERIES = TimestampSeries.of("default");
    private static final TimestampSeries SERIES_1 = TimestampSeries.of("series1");
    private static final TimestampSeries SERIES_2 = TimestampSeries.of("series2");

    private static final long FIFTY_MILLION = 50_000_000L;
    private static final long ONE_BILLION = 1_000_000_000L;

    protected abstract TimestampBoundStore createSingleSeriesTimestampBoundStore();

    protected abstract TimestampBoundStore createMultiSeriesTimestampBoundStore(TimestampSeries series);

    protected abstract TimestampSeriesProvider createTimestampSeriesProvider();

    @Test
    public void multipleSeriesAreDistinct() {
        TimestampBoundStore store1 = createMultiSeriesTimestampBoundStore(SERIES_1);
        TimestampBoundStore store2 = createMultiSeriesTimestampBoundStore(SERIES_2);

        long initialValue1 = store1.getUpperLimit();
        long initialValue2 = store2.getUpperLimit();

        store1.storeUpperLimit(initialValue1 + 1);
        assertThat(store1.getUpperLimit()).isEqualTo(initialValue1 + 1);
        assertThat(store2.getUpperLimit()).isEqualTo(initialValue2);

        store2.storeUpperLimit(initialValue2 + 2);
        assertThat(store1.getUpperLimit()).isEqualTo(initialValue1 + 1);
        assertThat(store2.getUpperLimit()).isEqualTo(initialValue2 + 2);
    }

    @Test
    public void multiSeriesWritesDoNotAffectLegacyStore() {
        TimestampBoundStore legacyStore = createSingleSeriesTimestampBoundStore();
        TimestampBoundStore newStore = createMultiSeriesTimestampBoundStore(DEFAULT_SERIES);
        long initialValue = newStore.getUpperLimit();
        legacyStore.storeUpperLimit(FIFTY_MILLION);

        assertThat(legacyStore.getUpperLimit()).isEqualTo(FIFTY_MILLION);
        assertThat(newStore.getUpperLimit()).isEqualTo(initialValue);

        newStore.storeUpperLimit(ONE_BILLION);

        assertThat(legacyStore.getUpperLimit()).isEqualTo(FIFTY_MILLION);
        assertThat(newStore.getUpperLimit()).isEqualTo(ONE_BILLION);
    }

    @Test
    public void reportsSingleKnownSeries() {
        createMultiSeriesTimestampBoundStore(SERIES_1).storeUpperLimit(2_345_678);
        assertThat(createTimestampSeriesProvider().getKnownSeries()).contains(SERIES_1);
    }

    @Test
    public void canReportMultipleSeries() {
        Set<String> clients = IntStream.range(0, 100)
                .mapToObj(unused -> UUID.randomUUID().toString())
                .collect(Collectors.toSet());
        clients.forEach(client -> {
            TimestampBoundStore store = createMultiSeriesTimestampBoundStore(TimestampSeries.of(client));
            store.storeUpperLimit(1_234_567);
        });

        Set<String> knownSeries = createTimestampSeriesProvider().getKnownSeries().stream()
                .map(TimestampSeries::series)
                .collect(Collectors.toSet());
        assertThat(clients).isSubsetOf(knownSeries);
    }
}
