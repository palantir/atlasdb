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

import org.junit.After;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.timestamp.AbstractDbTimestampBoundStoreTest;
import com.palantir.timestamp.TimestampBoundStore;

public class PostgresMultiSeriesDbTimestampBoundStoreTest extends AbstractDbTimestampBoundStoreTest {
    private static final String DEFAULT_SERIES = "defaultSeries";
    private static final String SERIES_1 = "series1";
    private static final String SERIES_2 = "series2";

    private static final long FIFTY_MILLION = 50_000_000L;
    private static final long ONE_BILLION = 1_000_000_000L;

    private ConnectionManagerAwareDbKvs kvs;

    @After
    public void tearDown() {
        kvs.close();
    }

    @Override
    protected TimestampBoundStore createTimestampBoundStore() {
        kvs = DbkvsPostgresTestSuite.createKvs();
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
                DbkvsPostgresTestSuite.getKvsConfig().ddl().tablePrefix());
        long initialValue = store.getUpperLimit();
        legacyStore.storeUpperLimit(FIFTY_MILLION);

        assertThat(legacyStore.getUpperLimit()).isEqualTo(FIFTY_MILLION);
        assertThat(store.getUpperLimit()).isEqualTo(initialValue);

        store.storeUpperLimit(ONE_BILLION);

        assertThat(legacyStore.getUpperLimit()).isEqualTo(FIFTY_MILLION);
        assertThat(store.getUpperLimit()).isEqualTo(ONE_BILLION);
    }

    private InDbTimestampBoundStore createDbTimestampBoundStore(String series) {
        return InDbTimestampBoundStore.createMultiTableForSeries(
                kvs.getConnectionManager(),
                AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE,
                series);
    }
}
