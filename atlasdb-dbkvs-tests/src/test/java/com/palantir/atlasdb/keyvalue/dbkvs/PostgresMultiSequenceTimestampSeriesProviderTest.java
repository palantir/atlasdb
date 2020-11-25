/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.MultiSequenceTimestampSeriesProvider;
import com.palantir.timestamp.TimestampBoundStore;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PostgresMultiSequenceTimestampSeriesProviderTest {
    private static final TimestampSeries DEFAULT_SERIES = TimestampSeries.of("default");

    private ConnectionManagerAwareDbKvs kvs;
    private TimestampBoundStore boundStore;
    private TimestampSeriesProvider seriesProvider;

    @Before
    public void createTimestampInterfaces() {
        kvs = DbkvsPostgresTestSuite.createKvs();
        boundStore = createDbTimestampBoundStore(kvs, DEFAULT_SERIES);
        seriesProvider = createTimestampSeriesProvider(kvs);
    }

    @After
    public void tearDown() {
        kvs.close();
    }

    @Test
    public void reportsSingleKnownSeries() {
        boundStore.storeUpperLimit(2_345_678);
        assertThat(seriesProvider.getKnownSeries()).contains(DEFAULT_SERIES);
    }

    @Test
    public void canReportMultipleSeries() {
        Set<String> clients = IntStream.range(0, 100)
                .mapToObj(unused -> UUID.randomUUID().toString())
                .collect(Collectors.toSet());
        clients.forEach(client -> {
            TimestampBoundStore store = createDbTimestampBoundStore(kvs, TimestampSeries.of(client));
            store.storeUpperLimit(1_234_567);
        });

        Set<String> knownSeries = seriesProvider.getKnownSeries().stream()
                .map(TimestampSeries::series)
                .collect(Collectors.toSet());
        assertThat(clients).isSubsetOf(knownSeries);
    }

    private static TimestampBoundStore createDbTimestampBoundStore(
            ConnectionManagerAwareDbKvs keyValueService, TimestampSeries series) {
        return InDbTimestampBoundStore.createForMultiSeries(
                keyValueService.getConnectionManager(), AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, series);
    }

    private static TimestampSeriesProvider createTimestampSeriesProvider(ConnectionManagerAwareDbKvs keyValueService) {
        return MultiSequenceTimestampSeriesProvider.create(
                keyValueService, AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, false);
    }
}
