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

import java.util.NoSuchElementException;
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
    private static final String ROW_AND_COLUMN_NAME = "ts";
    private static final Cell TIMESTAMP_BOUND_CELL =
            Cell.create(PtBytes.toBytes(ROW_AND_COLUMN_NAME), PtBytes.toBytes(ROW_AND_COLUMN_NAME));
    private static final long OFFSET = 100000;
    private static final long GREATER_OFFSET = OFFSET + 1;


    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraTimestampBoundStoreIntegrationTest.class)
            .with(new CassandraContainer());

    private final CassandraKeyValueService kv = CassandraKeyValueService.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG);

    @Override
    public TimestampBoundStore createTimestampBoundStore() {
        return CassandraTimestampBoundStore.create(kv);
    }

    @Test
    public void storeWithNoTableOnStartupThrows() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatStoreUpperLimitThrows(OFFSET, IllegalStateException.class);
    }

    @Test
    public void storeWithEmptyTableOnStartupThrows() {
        assertThatStoreUpperLimitThrows(OFFSET, IllegalStateException.class);
    }

    @Test
    public void storeWithNoIdOnStartupThrows() {
        insertTimestampOld(OFFSET);
        assertThatStoreUpperLimitThrows(GREATER_OFFSET, IllegalStateException.class);
    }

    @Test
    public void storeWithWrongIdOnStartupThrows() {
        insertTimestampWithFakeId(OFFSET);
        assertThatStoreUpperLimitThrows(GREATER_OFFSET, IllegalStateException.class);
    }

    @Test
    public void getWithNoTableOnStartupThrows() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatGetUpperLimitThrows(PalantirRuntimeException.class);
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
        assertThatGetUpperLimitThrows(MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getWithNoIdAfterMigrationThrows() {
        insertTimestampOld(OFFSET);
        store.getUpperLimit();
        insertTimestampOld(OFFSET);
        assertThatGetUpperLimitThrows(MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getWithWrongIdAfterMigrationThrows() {
        insertTimestampWithFakeId(OFFSET);
        store.getUpperLimit();
        insertTimestampWithFakeId(OFFSET);
        assertThatGetUpperLimitThrows(MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getWithSmallerThanExpectedTimestampThrows() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit - 1);
        assertThatGetUpperLimitThrows(IllegalArgumentException.class);
    }

    @Test
    public void getWithGreaterThanExpectedTimestampSucceeds() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit + OFFSET);
        assertThat(store.getUpperLimit()).isEqualTo(limit + OFFSET);
    }

    @Test
    public void storeWithEmptyTableAfterMigrationThrows() {
        long limit = store.getUpperLimit();
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatStoreUpperLimitThrows(limit + OFFSET, NoSuchElementException.class);
    }

    @Test
    public void storeWithNoIdAfterMigrationThrows() {
        insertTimestampOld(OFFSET);
        long limit = store.getUpperLimit();
        insertTimestampOld(OFFSET);
        assertThatStoreUpperLimitThrows(limit + OFFSET, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void storeWithWrongIdAfterMigrationThrows() {
        insertTimestampWithFakeId(OFFSET);
        long limit = store.getUpperLimit();
        insertTimestampWithFakeId(OFFSET);
        assertThatStoreUpperLimitThrows(limit + OFFSET, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void storeSmallerThanExpectedTimestampThrows() {
        long limit = store.getUpperLimit();
        assertThatStoreUpperLimitThrows(limit - 1, IllegalArgumentException.class);
    }

    @Test
    public void storeLowerTimestampThanUnexpectedInDbThrows() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit + GREATER_OFFSET);
        assertThatStoreUpperLimitThrows(limit + OFFSET, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void storeGreaterTimestampThanUnexpectedInDbSucceeds() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit + OFFSET);
        store.storeUpperLimit(limit + GREATER_OFFSET);
        assertGetReturnsBoundEqualTo(limit + GREATER_OFFSET);
    }

    @Test
    public void backwardsCompatibilityWhenReadingBoundFromDb() {
        long limit = store.getUpperLimit();
        store.storeUpperLimit(limit + OFFSET);
        assertThat(getBoundFromDb()).isEqualTo(limit + OFFSET);
    }

    @Test
    public void valueBelowEightBytesThrows() {
        setTimestampTableValueTo(PtBytes.toBytes("1"));
        assertThatGetUpperLimitThrows(IllegalArgumentException.class);
    }

    @Test
    public void invalidValueAboveEightBytesThrows() {
        setTimestampTableValueTo(PtBytes.toBytes("123456789"));
        assertThatGetUpperLimitThrows(IllegalArgumentException.class);
    }

    @Test
    public void invalidValueAboveTwentyFourBytesThrows() {
        setTimestampTableValueTo(PtBytes.toBytes("123456789123456789123456789"));
        assertThatGetUpperLimitThrows(IllegalArgumentException.class);
    }

    @Test
    public void testBounds() {
        long limit = store.getUpperLimit();
        store.storeUpperLimit(limit + 10);
        assertGetReturnsBoundEqualTo(limit + 10);
        store.storeUpperLimit(limit + 20);
        assertGetReturnsBoundEqualTo(limit + 20);
        store.storeUpperLimit(limit + 30);
        assertGetReturnsBoundEqualTo(limit + 30);
    }

    @Test
    public void resilientToMultipleStoresBeforeGet() {
        TimestampBoundStore ts = CassandraTimestampBoundStore.create(kv);
        long limit = ts.getUpperLimit();
        ts.storeUpperLimit(limit + 10);
        ts.storeUpperLimit(limit + 20);
        assertThat(ts.getUpperLimit()).isEqualTo(limit + 20);
    }

    @Test
    public void testOnlyOneTimestampBoundStoreCanBeActive() {
        TimestampBoundStore ts2 = CassandraTimestampBoundStore.create(kv);
        long limit = ts2.getUpperLimit();

        assertGetReturnsBoundEqualTo(limit);
        store.storeUpperLimit(limit + 10);
        assertGetReturnsBoundEqualTo(limit + 10);
        assertThatThrownBy(() -> ts2.getUpperLimit()).isExactlyInstanceOf(MultipleRunningTimestampServiceError.class);

        store.storeUpperLimit(limit + 20);
        assertThatThrownBy(() -> ts2.storeUpperLimit(limit + 20))
                .isExactlyInstanceOf(MultipleRunningTimestampServiceError.class);
        assertGetReturnsBoundEqualTo(limit + 20);
        assertThatThrownBy(() -> ts2.getUpperLimit()).isExactlyInstanceOf(MultipleRunningTimestampServiceError.class);

        store.storeUpperLimit(limit + 30);
        assertGetReturnsBoundEqualTo(limit + 30);

        assertThatThrownBy(() -> ts2.storeUpperLimit(limit + 40))
                .isExactlyInstanceOf(MultipleRunningTimestampServiceError.class);
    }

    @After
    public void cleanUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.close();
    }

    private void assertGetReturnsBoundEqualTo(long limit) {
        assertThat(store.getUpperLimit()).isEqualTo(limit);
    }

    private void assertThatStoreUpperLimitThrows(long limit, Class exception) {
        assertThatThrownBy(() -> store.storeUpperLimit(limit)).isExactlyInstanceOf(exception);
    }

    private void assertThatGetUpperLimitThrows(Class exception) {
        assertThatThrownBy(() -> store.getUpperLimit()).isExactlyInstanceOf(exception);
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
        return TimestampBoundStoreEntry.createFromBytes(value.getContents()).timestamp();
    }

    private UUID getIdFromDb() {
        Value value = kv.get(AtlasDbConstants.TIMESTAMP_TABLE, ImmutableMap.of(TIMESTAMP_BOUND_CELL, Long.MAX_VALUE))
                .get(TIMESTAMP_BOUND_CELL);
        return TimestampBoundStoreEntry.createFromBytes(value.getContents()).id();
    }

    private UUID getStoreId(TimestampBoundStore store) {
        return ((CassandraTimestampBoundStore) store).getId();
    }
}
