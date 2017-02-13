/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.timestamp.AbstractDbTimestampBoundStoreTest;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public class CassandraTimestampBoundStoreIntegrationTest extends AbstractDbTimestampBoundStoreTest {
    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final Cell TIMESTAMP_BOUND_CELL = Cell.create(
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME),
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME));
    private static final long OFFSET = 100000;
    private static final long GREATER_OFFSET = OFFSET + 1;

    private final CassandraKeyValueService kv = CassandraKeyValueService.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG);

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraTimestampBoundStoreIntegrationTest.class)
            .with(new CassandraContainer());

    @Override
    public TimestampBoundStore createTimestampBoundStore() {
        return CassandraTimestampBoundStore.create(kv);
    }

    @Test
    public void storeWithNoTableOnStartupThrows() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatStoreUpperLimitThrows(store, OFFSET, PalantirRuntimeException.class);
    }

    @Test
    public void storeWithEmptyTableOnStartupThrows() {
        assertThatStoreUpperLimitThrows(store, OFFSET, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void storeWithNoIdOnStartupThrows() {
        insertTimestampOld(OFFSET);
        assertThatStoreUpperLimitThrows(store, GREATER_OFFSET, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void storeWithWrongIdOnStartupThrows() {
        insertTimestampWithFakeId(OFFSET);
        assertThatStoreUpperLimitThrows(store, GREATER_OFFSET, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getWithNoTableOnStartupThrows() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatGetUpperLimitThrows(store, PalantirRuntimeException.class);
    }

    @Test
    public void canMigrateFromEmptyTable() {
        store.getUpperLimit();
        assertThat(getIdFromDb()).isEqualTo(getStoreId(store));
    }

    @Test
    public void canMigrateFromNoId() {
        insertTimestampOld(OFFSET);
        store.getUpperLimit();
        assertThat(getBoundFromDb()).isEqualTo(OFFSET);
        assertThat(getIdFromDb()).isEqualTo(getStoreId(store));
    }

    @Test
    public void canMigrateFromWrongId() {
        insertTimestampWithFakeId(OFFSET);
        store.getUpperLimit();
        assertThat(getBoundFromDb()).isEqualTo(OFFSET);
        assertThat(getIdFromDb()).isEqualTo(getStoreId(store));
    }

    @Test
    public void getWithEmptyTableAfterMigrationThrows() {
        store.getUpperLimit();
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatGetUpperLimitThrows(store, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getWithNoIdAfterMigrationThrows() {
        insertTimestampOld(OFFSET);
        store.getUpperLimit();
        insertTimestampOld(OFFSET);
        assertThatGetUpperLimitThrows(store, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getWithWrongIdAfterMigrationThrows() {
        insertTimestampWithFakeId(OFFSET);
        store.getUpperLimit();
        insertTimestampWithFakeId(OFFSET);
        assertThatGetUpperLimitThrows(store, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getWithSmallerThanExpectedTimestampThrows() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit - 1);
        assertThatGetUpperLimitThrows(store, IllegalArgumentException.class);
    }

    @Test
    public void getWithGreaterThanExpectedTimestampSucceeds() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit + OFFSET);
        assertThatGetReturnsBoundEqualTo(limit + OFFSET);
    }

    @Test
    public void storeWithEmptyTableAfterMigrationThrows() {
        long limit = store.getUpperLimit();
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatStoreUpperLimitThrows(store, limit + OFFSET, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void storeWithNoIdAfterMigrationThrows() {
        insertTimestampOld(OFFSET);
        long limit = store.getUpperLimit();
        insertTimestampOld(OFFSET);
        assertThatStoreUpperLimitThrows(store, limit + OFFSET, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void storeWithWrongIdAfterMigrationThrows() {
        insertTimestampWithFakeId(OFFSET);
        long limit = store.getUpperLimit();
        insertTimestampWithFakeId(OFFSET);
        assertThatStoreUpperLimitThrows(store, limit + OFFSET, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void storeSmallerThanExpectedTimestampThrows() {
        long limit = store.getUpperLimit();
        assertThatStoreUpperLimitThrows(store, limit - 1, IllegalArgumentException.class);
    }

    @Test
    public void storeLowerTimestampThanUnexpectedInDbThrows() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit + GREATER_OFFSET);
        assertThatStoreUpperLimitThrows(store, limit + OFFSET, IllegalArgumentException.class);
    }

    @Test
    public void storeGreaterTimestampThanUnexpectedInDbSucceeds() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit + OFFSET);
        store.storeUpperLimit(limit + GREATER_OFFSET);
        assertThatGetReturnsBoundEqualTo(limit + GREATER_OFFSET);
    }

    @Test
    public void backwardsCompatibilityWhenReadingBoundFromDb() {
        long limit = store.getUpperLimit();
        store.storeUpperLimit(limit + OFFSET);
        assertThat(getBoundFromDb()).isEqualTo(limit + OFFSET);
    }

    @Test
    public void valueBelowEightBytesThrows() {
        setTimestampTableValueTo(new byte[1]);
        assertThatGetUpperLimitThrows(store, IllegalArgumentException.class);
    }

    @Test
    public void invalidValueAboveEightBytesThrows() {
        setTimestampTableValueTo(new byte[9]);
        assertThatGetUpperLimitThrows(store, IllegalArgumentException.class);
    }

    @Test
    public void invalidValueAboveTwentyFourBytesThrows() {
        setTimestampTableValueTo(new byte[25]);
        assertThatGetUpperLimitThrows(store, IllegalArgumentException.class);
    }

    @Test
    public void canGetLastStoredBound() {
        long limit = store.getUpperLimit();
        store.storeUpperLimit(limit + 10);
        assertThatGetReturnsBoundEqualTo(limit + 10);
        store.storeUpperLimit(limit + 20);
        assertThatGetReturnsBoundEqualTo(limit + 20);
        store.storeUpperLimit(limit + 30);
        assertThatGetReturnsBoundEqualTo(limit + 30);
    }

    @Test
    public void resilientToMultipleStoreUpperLimitBeforeGet() {
        TimestampBoundStore ts = CassandraTimestampBoundStore.create(kv);
        long limit = ts.getUpperLimit();
        ts.storeUpperLimit(limit + 10);
        ts.storeUpperLimit(limit + 20);
        assertThatGetReturnsBoundEqualTo(limit + 20);
    }

    @Test
    public void latestInitialisedStoreCanStoreAndGetOthersThrow() {
        TimestampBoundStore ts = CassandraTimestampBoundStore.create(kv);
        long limit = ts.getUpperLimit();

        assertThatGetReturnsBoundEqualTo(limit);
        store.storeUpperLimit(limit + 10);
        assertThatGetReturnsBoundEqualTo(limit + 10);
        assertThatGetUpperLimitThrows(ts, MultipleRunningTimestampServiceError.class);

        store.storeUpperLimit(limit + 20);
        assertThatStoreUpperLimitThrows(ts, limit + 20, MultipleRunningTimestampServiceError.class);
        assertThatGetReturnsBoundEqualTo(limit + 20);
        assertThatGetUpperLimitThrows(ts, MultipleRunningTimestampServiceError.class);

        store.storeUpperLimit(limit + 30);
        assertThatGetReturnsBoundEqualTo(limit + 30);
        assertThatStoreUpperLimitThrows(ts, limit + 40, MultipleRunningTimestampServiceError.class);
    }

    @After
    public void cleanUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.close();
    }

    private void assertThatGetReturnsBoundEqualTo(long limit) {
        assertThat(store.getUpperLimit()).isEqualTo(limit);
    }

    private void assertThatStoreUpperLimitThrows(TimestampBoundStore st, long limit, Class exception) {
        assertThatThrownBy(() -> st.storeUpperLimit(limit)).isExactlyInstanceOf(exception);
    }

    private void assertThatGetUpperLimitThrows(TimestampBoundStore st, Class exception) {
        assertThatThrownBy(st::getUpperLimit).isExactlyInstanceOf(exception);
    }

    private void insertTimestampWithCorrectId(long value) {
        insertTimestampWithIdChanged(value, false);
    }

    private void insertTimestampWithFakeId(long value) {
        insertTimestampWithIdChanged(value, true);
    }

    private void insertTimestampOld(long value) {
        setTimestampTableValueTo(PtBytes.toBytes(value));
    }

    private void insertTimestampWithIdChanged(long value, boolean changeId) {
        UUID id = getStoreId(store);
        if (changeId) {
            id = new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits() ^ 1);
        }
        setTimestampTableValueTo(TimestampBoundStoreEntry.getByteValueForIdAndBound(id, value));
    }

    private void setTimestampTableValueTo(byte[] data) {
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.put(AtlasDbConstants.TIMESTAMP_TABLE, ImmutableMap.of(TIMESTAMP_BOUND_CELL, data), CASSANDRA_TIMESTAMP);
    }

    private long getBoundFromDb() {
        Value value = kv.get(AtlasDbConstants.TIMESTAMP_TABLE, ImmutableMap.of(TIMESTAMP_BOUND_CELL, Long.MAX_VALUE))
                .get(TIMESTAMP_BOUND_CELL);
        return TimestampBoundStoreEntry.createFromBytes(value.getContents()).timestamp().get();
    }

    private UUID getIdFromDb() {
        Value value = kv.get(AtlasDbConstants.TIMESTAMP_TABLE, ImmutableMap.of(TIMESTAMP_BOUND_CELL, Long.MAX_VALUE))
                .get(TIMESTAMP_BOUND_CELL);
        return TimestampBoundStoreEntry.createFromBytes(value.getContents()).id().get();
    }

    private UUID getStoreId(TimestampBoundStore store) {
        return ((CassandraTimestampBoundStore) store).getId();
    }
}
