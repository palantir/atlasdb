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
    private static final String ROW_AND_COLUMN_NAME = "ts";
    private static final Cell TIMESTAMP_BOUND_CELL =
            Cell.create(PtBytes.toBytes(ROW_AND_COLUMN_NAME), PtBytes.toBytes(ROW_AND_COLUMN_NAME));
    private static final long OFFSET = 100;
    private static final long SECOND_OFFSET = OFFSET + 1;


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
    public void storeWithEmptyTableOnStartupSucceeds() {
        store.storeUpperLimit(OFFSET);
        assertStoreReturnsBoundEqualTo(OFFSET);
    }

    @Test
    public void storeWithEmptyTableThrowsAfterStore() {
        store.storeUpperLimit(OFFSET);
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatStoreUpperLimitThrows(SECOND_OFFSET);
    }

    @Test
    public void storeWithNonEmptyTableNoIdOnStartupThrows() {
        insertTimestampOld(OFFSET);
        assertThatStoreUpperLimitThrows(SECOND_OFFSET);
    }

    @Test
    public void storeWithNonEmptyTableWrongIdOnStartupThrows() {
        insertTimestampWithFakeId(OFFSET);
        assertThatStoreUpperLimitThrows(SECOND_OFFSET);
    }

    @Test
    public void getOrStoreWithNoTableThrows() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatGetUpperLimitThrows(PalantirRuntimeException.class);
        assertThatThrownBy(() -> store.storeUpperLimit(OFFSET)).isExactlyInstanceOf(PalantirRuntimeException.class);
    }

    @Test
    public void getNewFormatOnStartupSucceeds() {
        insertTimestampWithCorrectId(OFFSET);
        assertStoreReturnsBoundEqualTo(OFFSET);
    }

    @Test
    public void getNewFormatAfterStoreThrows() {
        store.getUpperLimit();
        store.storeUpperLimit(SECOND_OFFSET);
        insertTimestampWithFakeId(OFFSET);
        assertThatGetUpperLimitThrows(MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void getOldFormatOnStartupSucceeds() {
        insertTimestampOld(OFFSET);
        assertStoreReturnsBoundEqualTo(OFFSET);
    }

    @Test
    public void getOldFormatAfterStoreThrows() {
        store.getUpperLimit();
        store.storeUpperLimit(SECOND_OFFSET);
        insertTimestampOld(OFFSET);
        assertThatGetUpperLimitThrows(MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void storeWithWrongTimestampCorrectIdSucceeds() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit + OFFSET);
        store.storeUpperLimit(limit + SECOND_OFFSET);
        assertStoreReturnsBoundEqualTo(limit + SECOND_OFFSET);
    }

    @Test
    public void storeWithWrongTimestampNoIdThrows() {
        long limit = store.getUpperLimit();
        insertTimestampOld(limit + OFFSET);
        assertThatStoreUpperLimitThrows(limit + SECOND_OFFSET);
    }

    @Test
    public void storeWithWrongTimestampWrongIdThrows() {
        long limit = store.getUpperLimit();
        insertTimestampWithFakeId(limit + OFFSET);
        assertThatStoreUpperLimitThrows(limit + SECOND_OFFSET);
    }

    @Test
    public void storeWithRightTimestampNoIdSucceedsFirstTime() {
        insertTimestampOld(OFFSET);
        assertThat(store.getUpperLimit()).isEqualTo(OFFSET);
        store.storeUpperLimit(SECOND_OFFSET);
        assertStoreReturnsBoundEqualTo(SECOND_OFFSET);
    }

    @Test
    public void storeWithRightTimestampNoIdSecondTimeThrows() {
        insertTimestampOld(OFFSET);
        store.getUpperLimit();
        store.storeUpperLimit(SECOND_OFFSET);
        insertTimestampOld(OFFSET);
        assertThatStoreUpperLimitThrows(SECOND_OFFSET);
    }

    @Test
    public void storeWithRightTimestampWrongIdFirstTimeSucceeds() {
        insertTimestampWithFakeId(OFFSET);
        assertThat(store.getUpperLimit()).isEqualTo(OFFSET);
        store.storeUpperLimit(SECOND_OFFSET);
        assertStoreReturnsBoundEqualTo(SECOND_OFFSET);
    }

    @Test
    public void storeWithRightTimestampWrongIdSecondTimeThrows() {
        insertTimestampWithFakeId(OFFSET);
        store.getUpperLimit();
        store.storeUpperLimit(SECOND_OFFSET);
        insertTimestampWithFakeId(OFFSET);
        assertThatStoreUpperLimitThrows(SECOND_OFFSET);
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

    /*
     * Merged in the following two tests from the old CassandraTimestampIntegrationTest
     */
    @Test
    public void testBounds() {
        long limit = store.getUpperLimit();
        store.storeUpperLimit(limit + 10);
        assertStoreReturnsBoundEqualTo(limit + 10);
        store.storeUpperLimit(limit + 20);
        assertStoreReturnsBoundEqualTo(limit + 20);
        store.storeUpperLimit(limit + 30);
        assertStoreReturnsBoundEqualTo(limit + 30);
    }

    @Test
    public void testMultipleThrows() {
        TimestampBoundStore ts2 = CassandraTimestampBoundStore.create(kv);
        long limit = store.getUpperLimit();

        assertThat(ts2.getUpperLimit()).isEqualTo(limit);
        store.storeUpperLimit(limit + 10);
        assertStoreReturnsBoundEqualTo(limit + 10);
        assertThat(ts2.getUpperLimit()).isEqualTo(limit + 10);

        store.storeUpperLimit(limit + 20);
        assertThatThrownBy(() -> ts2.storeUpperLimit(limit + 20))
                .isExactlyInstanceOf(MultipleRunningTimestampServiceError.class);
        assertStoreReturnsBoundEqualTo(limit + 20);
        assertThat(ts2.getUpperLimit()).isEqualTo(limit + 20);

        store.storeUpperLimit(limit + 30);
        assertStoreReturnsBoundEqualTo(limit + 30);

        assertThatThrownBy(() -> ts2.storeUpperLimit(limit + 40))
                .isExactlyInstanceOf(MultipleRunningTimestampServiceError.class);
    }

    @After
    public void cleanUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.close();
    }

    private void assertStoreReturnsBoundEqualTo(long limit) {
        assertThat(store.getUpperLimit()).isEqualTo(limit);
    }

    private void assertThatStoreUpperLimitThrows(long limit) {
        assertThatThrownBy(() -> store.storeUpperLimit(limit))
                .isExactlyInstanceOf(MultipleRunningTimestampServiceError.class);
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
        UUID id = ((CassandraTimestampBoundStore) store).getId();
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
}
