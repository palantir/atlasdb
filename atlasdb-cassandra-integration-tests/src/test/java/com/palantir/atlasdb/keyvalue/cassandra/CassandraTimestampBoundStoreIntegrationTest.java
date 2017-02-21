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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.timestamp.AbstractDbTimestampBoundStoreTest;
import com.palantir.common.base.Throwables;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public class CassandraTimestampBoundStoreIntegrationTest extends AbstractDbTimestampBoundStoreTest {
    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final Cell OLD_TIMESTAMP_BOUND_CELL = Cell.create(
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME),
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME));
    private static final Cell NEW_TIMESTAMP_BOUND_CELL = Cell.create(
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME),
            PtBytes.toBytes(CassandraTimestampUtils.ID_ROW_AND_COLUMN_NAME));
    private static final long OFFSET = CassandraTimestampUtils.INITIAL_VALUE + 100301;
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

    @After
    public void cleanUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.close();
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
        leaveOnlyOldTimestampFormat(OFFSET);
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
        getUpperLimitAndCheckEntriesInDb(CassandraTimestampUtils.INITIAL_VALUE);
    }

    @Test
    public void canMigrateFromOnlyOldFormatTimestamp() {
        leaveOnlyOldTimestampFormat(OFFSET);
        getUpperLimitAndCheckEntriesInDb(OFFSET);
    }

    @Test
    public void canMigrateFromWrongId() {
        insertTimestampWithFakeId(OFFSET);
        getUpperLimitAndCheckEntriesInDb(OFFSET);
    }

    @Test
    public void canMigrateFromOnlyNewFormatTimestamp() {
        leaveOnlyNewTimestampFormat(OFFSET);
        getUpperLimitAndCheckEntriesInDb(OFFSET);
    }

    @Test
    public void canMigrateFromMismatchedTimestampInTwoPersistentFormatsToGreaterBoundA() {
        setOldAndNewFormatForTimestamp(OFFSET, GREATER_OFFSET);
        getUpperLimitAndCheckEntriesInDb(GREATER_OFFSET);
    }

    @Test
    public void canMigrateFromMismatchedTimestampInTwoPersistentFormatsToGreaterBoundB() {
        setOldAndNewFormatForTimestamp(GREATER_OFFSET, OFFSET);
        getUpperLimitAndCheckEntriesInDb(GREATER_OFFSET);
    }

    @Test
    public void canMigrateFromHotfixedVersion() {
        setTimestampTableOldColumnValue(TimestampBoundStoreEntry.getByteValueForIdAndBound(UUID.randomUUID(), OFFSET));
        getUpperLimitAndCheckEntriesInDb(OFFSET);
    }

    @Test
    public void getWithEmptyTableAfterMigrationThrows() {
        store.getUpperLimit();
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatGetUpperLimitThrows(store, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getWithNoIdAfterMigrationThrows() {
        leaveOnlyOldTimestampFormat(OFFSET);
        store.getUpperLimit();
        leaveOnlyOldTimestampFormat(OFFSET);
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
    public void getWithMismatchedTimestampInTwoPersistentFormatsThrowsA() {
        setOldAndNewFormatForTimestamp(OFFSET, GREATER_OFFSET);
        store.getUpperLimit();
        setOldAndNewFormatForTimestamp(OFFSET, GREATER_OFFSET);
        assertThatGetUpperLimitThrows(store, MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getWithMismatchedTimestampInTwoPersistentFormatsThrowsB() {
        setOldAndNewFormatForTimestamp(GREATER_OFFSET, OFFSET);
        store.getUpperLimit();
        setOldAndNewFormatForTimestamp(GREATER_OFFSET, OFFSET);
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
        leaveOnlyOldTimestampFormat(OFFSET);
        long limit = store.getUpperLimit();
        leaveOnlyOldTimestampFormat(OFFSET);
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
    public void storeWithMismatchedTimestampInTwoPersistentFormatsThrows() {
        setOldAndNewFormatForTimestamp(OFFSET, GREATER_OFFSET);
        store.getUpperLimit();
        setOldAndNewFormatForTimestamp(OFFSET, GREATER_OFFSET);
        assertThatStoreUpperLimitThrows(store, GREATER_OFFSET + OFFSET, MultipleRunningTimestampServiceError.class);
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
        assertThat(getOldBoundFromDb()).isEqualTo(limit + OFFSET);
    }

    @Test
    public void canRollbackToOldVersion() {
        long limit = store.getUpperLimit();
        store.storeUpperLimit(limit + OFFSET);

        MinimalOldCassandraTimestampBoundStore oldStore = new MinimalOldCassandraTimestampBoundStore();
        assertThat(oldStore.getUpperLimit()).isEqualTo(limit + OFFSET);
        oldStore.storeUpperLimit(limit + GREATER_OFFSET);
        assertThat(oldStore.getUpperLimit()).isEqualTo(limit + GREATER_OFFSET);
    }

    @Test
    public void canMigrateBackAfterRollback() {
        long limit = store.getUpperLimit();

        MinimalOldCassandraTimestampBoundStore oldStore = new MinimalOldCassandraTimestampBoundStore();
        assertThat(oldStore.getUpperLimit()).isEqualTo(limit);
        oldStore.storeUpperLimit(limit + OFFSET);
        assertThat(oldStore.getUpperLimit()).isEqualTo(limit + OFFSET);

        TimestampBoundStore newts = CassandraTimestampBoundStore.create(kv);
        assertThat(newts.getUpperLimit()).isEqualTo(limit + OFFSET);
        newts.storeUpperLimit(limit + GREATER_OFFSET);
        assertThat(newts.getUpperLimit()).isEqualTo(limit + GREATER_OFFSET);
    }

    @Test
    public void migrationWithValueOfOneByteThrows() {
        setTimestampTableOldColumnValue(new byte[1]);
        assertThatGetUpperLimitThrows(store, IllegalArgumentException.class);
    }

    @Test
    public void migrationWithValueOfNineBytesThrows() {
        setTimestampTableOldColumnValue(new byte[9]);
        assertThatGetUpperLimitThrows(store, IllegalArgumentException.class);
    }

    @Test
    public void migrationWithValueOfTwentyFiveBytesThrows() {
        setTimestampTableOldColumnValue(new byte[25]);
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

    private void getUpperLimitAndCheckEntriesInDb(long value) {
        store.getUpperLimit();
        assertThat(getOldBoundFromDb()).isEqualTo(value);
        assertThat(getNewBoundFromDb()).isEqualTo(value);
        assertThat(getIdFromDb()).isEqualTo(getStoreId(store));
        assertThatGetReturnsBoundEqualTo(value);
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

    private void leaveOnlyOldTimestampFormat(long value) {
        setTimestampTableOldColumnValue(PtBytes.toBytes(value));
    }

    private void leaveOnlyNewTimestampFormat(long value) {
        setTimestampTableNewColumnValueTo(TimestampBoundStoreEntry.getByteValueForIdAndBound(getStoreId(store), value));
    }

    private void setOldAndNewFormatForTimestamp(long valueNew, long valueOld) {
        setTimestampTableColumnValuesTo(TimestampBoundStoreEntry.getByteValueForIdAndBound(null, valueNew),
                TimestampBoundStoreEntry.getByteValueForIdAndBound(getStoreId(store), valueOld));
    }

    private void insertTimestampWithIdChanged(long value, boolean changeId) {
        UUID id = getStoreId(store);
        if (changeId) {
            id = new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits() ^ 1);
        }
        setTimestampTableColumnValuesTo(TimestampBoundStoreEntry.getByteValueForIdAndBound(null, value),
                TimestampBoundStoreEntry.getByteValueForIdAndBound(id, value));
    }

    private void setTimestampTableOldColumnValue(byte[] data) {
        insertTimestampEntries(ImmutableMap.of(OLD_TIMESTAMP_BOUND_CELL, data));
    }

    private void setTimestampTableNewColumnValueTo(byte[] data) {
        insertTimestampEntries(ImmutableMap.of(NEW_TIMESTAMP_BOUND_CELL, data));
    }

    private void setTimestampTableColumnValuesTo(byte[] oldData, byte[] newData) {
        insertTimestampEntries(ImmutableMap.of(OLD_TIMESTAMP_BOUND_CELL, oldData, NEW_TIMESTAMP_BOUND_CELL, newData));
    }

    private void insertTimestampEntries(Map<Cell, byte[]> data) {
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.put(AtlasDbConstants.TIMESTAMP_TABLE, data, CASSANDRA_TIMESTAMP);
    }

    private long getOldBoundFromDb() {
        Value value = kv.get(AtlasDbConstants.TIMESTAMP_TABLE,
                ImmutableMap.of(OLD_TIMESTAMP_BOUND_CELL, Long.MAX_VALUE))
                .get(OLD_TIMESTAMP_BOUND_CELL);
        return TimestampBoundStoreEntry.createFromBytes(value.getContents()).timestamp().get();
    }

    private long getNewBoundFromDb() {
        Value value = kv.get(AtlasDbConstants.TIMESTAMP_TABLE,
                ImmutableMap.of(NEW_TIMESTAMP_BOUND_CELL, Long.MAX_VALUE))
                .get(NEW_TIMESTAMP_BOUND_CELL);
        return TimestampBoundStoreEntry.createFromBytes(value.getContents()).timestamp().get();
    }

    private UUID getIdFromDb() {
        Value value = kv.get(AtlasDbConstants.TIMESTAMP_TABLE,
                ImmutableMap.of(NEW_TIMESTAMP_BOUND_CELL, Long.MAX_VALUE))
                .get(NEW_TIMESTAMP_BOUND_CELL);
        return TimestampBoundStoreEntry.createFromBytes(value.getContents()).id().get();
    }

    private UUID getStoreId(TimestampBoundStore store) {
        return ((CassandraTimestampBoundStore) store).getId();
    }

    private class MinimalOldCassandraTimestampBoundStore {
        private static final long CASSANDRA_TIMESTAMP = 0L;
        private static final String ROW_AND_COLUMN_NAME = "ts";
        private static final long INITIAL_VALUE = 10000L;
        private long currentLimit = -1;
        CassandraClientPool clientPool;

        MinimalOldCassandraTimestampBoundStore() {
            clientPool = kv.getClientPool();
        }

        public long getUpperLimit() {
            return clientPool.runWithRetry(client -> {
                ByteBuffer rowName = getRowName();
                ColumnPath columnPath = new ColumnPath(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName());
                columnPath.setColumn(getColumnName());
                ColumnOrSuperColumn result;
                try {
                    result = client.get(rowName, columnPath, ConsistencyLevel.LOCAL_QUORUM);
                } catch (NotFoundException e) {
                    result = null;
                } catch (Exception e) {
                    throw Throwables.throwUncheckedException(e);
                }
                if (result == null) {
                    cas(client, null, INITIAL_VALUE);
                    return INITIAL_VALUE;
                }
                Column column = result.getColumn();
                currentLimit = PtBytes.toLong(column.getValue());
                return currentLimit;
            });
        }

        public void storeUpperLimit(final long limit) {
            clientPool.runWithRetry(client -> {
                cas(client, currentLimit, limit);
                return null;
            });
        }

        private void cas(Cassandra.Client client, Long oldVal, long newVal) {
            final CASResult result;
            try {
                result = client.cas(
                        getRowName(),
                        AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                        oldVal == null ? ImmutableList.of() : ImmutableList.of(makeColumn(oldVal)),
                        ImmutableList.of(makeColumn(newVal)),
                        ConsistencyLevel.SERIAL,
                        ConsistencyLevel.EACH_QUORUM);
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            }
            if (!result.isSuccess()) {
                throw new MultipleRunningTimestampServiceError("MRTSE");
            } else {
                currentLimit = newVal;
            }
        }

        private Column makeColumn(long ts) {
            Column col = new Column();
            col.setName(getColumnName());
            col.setValue(PtBytes.toBytes(ts));
            col.setTimestamp(CASSANDRA_TIMESTAMP);
            return col;
        }

        private byte[] getColumnName() {
            return CassandraKeyValueServices
                    .makeCompositeBuffer(PtBytes.toBytes(ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP)
                    .array();
        }

        private ByteBuffer getRowName() {
            return ByteBuffer.wrap(PtBytes.toBytes(ROW_AND_COLUMN_NAME));
        }
    }
}
