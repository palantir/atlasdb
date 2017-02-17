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
package com.palantir.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.palantir.common.exception.PalantirRuntimeException;


public abstract class AbstractTimestampBoundStoreWithIdTest {
    protected AbstractTimestampBoundStoreWithId store;
    protected static final long CASSANDRA_TIMESTAMP = 0L;
    protected static final long OFFSET = 100000;
    protected static final long GREATER_OFFSET = OFFSET + 1;

    @Before
    public void setUp() throws Exception {
        store = createTimestampBoundStore();
    }

    @Test
    public void storeWithNoTableOnStartupThrows() {
        dropTimestampTable();
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
        dropTimestampTable();
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
        truncateTimestampTable();
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
        truncateTimestampTable();
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
        AbstractTimestampBoundStoreWithId ts = createTimestampBoundStore();
        long limit = ts.getUpperLimit();
        ts.storeUpperLimit(limit + 10);
        ts.storeUpperLimit(limit + 20);
        assertThatGetReturnsBoundEqualTo(limit + 20);
    }

    @Test
    public void latestInitialisedStoreCanStoreAndGetOthersThrow() {
        AbstractTimestampBoundStoreWithId ts = createTimestampBoundStore();
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

    @Test
    public void basicTimestampBoundStoreTest() {
        long limit = store.getUpperLimit();
        assertThat(store.getUpperLimit()).isEqualTo(limit);
        store.storeUpperLimit(limit + 1);
        assertThat(store.getUpperLimit()).isEqualTo(limit + 1);
    }

    @After
    public void cleanUp() {
        dropTimestampTable();
        closeKvs();
    }

    protected void assertThatGetReturnsBoundEqualTo(long limit) {
        assertThat(store.getUpperLimit()).isEqualTo(limit);
    }

    protected void assertThatStoreUpperLimitThrows(AbstractTimestampBoundStoreWithId st, long limit, Class exception) {
        assertThatThrownBy(() -> st.storeUpperLimit(limit)).isExactlyInstanceOf(exception);
    }

    protected void assertThatGetUpperLimitThrows(AbstractTimestampBoundStoreWithId st, Class exception) {
        assertThatThrownBy(st::getUpperLimit).isExactlyInstanceOf(exception);
    }

    protected void insertTimestampWithCorrectId(long value) {
        insertTimestampWithIdChanged(value, false);
    }

    protected void insertTimestampWithFakeId(long value) {
        insertTimestampWithIdChanged(value, true);
    }

    protected void insertTimestampOld(long value) {
        putEntryInTimestampTable(value, null);
    }

    protected void insertTimestampWithIdChanged(long value, boolean changeId) {
        UUID id = getStoreId(store);
        if (changeId) {
            id = new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits() ^ 1);
        }
        putEntryInTimestampTable(value, id);
    }

    protected UUID getStoreId(AbstractTimestampBoundStoreWithId store) {
        return store.getId();
    }

    protected abstract AbstractTimestampBoundStoreWithId createTimestampBoundStore();

    protected abstract void dropTimestampTable();

    protected abstract void truncateTimestampTable();

    protected abstract void closeKvs();

    protected abstract void putEntryInTimestampTable(long value, UUID id);

    protected abstract long getBoundFromDb();

    protected abstract UUID getIdFromDb();
}

