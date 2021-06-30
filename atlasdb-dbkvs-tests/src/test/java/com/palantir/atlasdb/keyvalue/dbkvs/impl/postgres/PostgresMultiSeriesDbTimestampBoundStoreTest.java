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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.timestamp.AbstractDbTimestampBoundStoreTest;
import com.palantir.timestamp.TimestampBoundStore;
import org.junit.After;
import org.junit.Test;

public class PostgresMultiSeriesDbTimestampBoundStoreTest extends AbstractDbTimestampBoundStoreTest {
    private static final TimestampSeries DEFAULT_SERIES = TimestampSeries.of("defaultSeries");
    private static final TimestampSeries SERIES_1 = TimestampSeries.of("series1");
    private static final TimestampSeries SERIES_2 = TimestampSeries.of("series2");

    private static final long FIFTY_MILLION = 50_000_000L;
    private static final long ONE_BILLION = 1_000_000_000L;

    private ConnectionManagerAwareDbKvs kvs;

    @After
    public void tearDown() {
        kvs.close();
    }

    @Override
    protected TimestampBoundStore createTimestampBoundStore() {
        kvs = DbKvsPostgresTestSuite.createKvs();
        return createDbTimestampBoundStore(DEFAULT_SERIES);
    }

    @Test
    public void multipleSeriesAreDistinct() {
        TimestampBoundStore store1 = createDbTimestampBoundStore(SERIES_1);
        TimestampBoundStore store2 = createDbTimestampBoundStore(SERIES_2);

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
    public void doesNotAffectLegacyStore() {
        TimestampBoundStore legacyStore = InDbTimestampBoundStore.create(
                kvs.getConnectionManager(),
                AtlasDbConstants.TIMESTAMP_TABLE,
                DbKvsPostgresTestSuite.getKvsConfig().ddl().tablePrefix());
        long initialValue = store.getUpperLimit();
        legacyStore.storeUpperLimit(FIFTY_MILLION);

        assertThat(legacyStore.getUpperLimit()).isEqualTo(FIFTY_MILLION);
        assertThat(store.getUpperLimit()).isEqualTo(initialValue);

        store.storeUpperLimit(ONE_BILLION);

        assertThat(legacyStore.getUpperLimit()).isEqualTo(FIFTY_MILLION);
        assertThat(store.getUpperLimit()).isEqualTo(ONE_BILLION);
    }

    private TimestampBoundStore createDbTimestampBoundStore(TimestampSeries series) {
        return InDbTimestampBoundStore.createForMultiSeries(
                kvs.getConnectionManager(), AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, series);
    }
}
