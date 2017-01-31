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
import com.palantir.atlasdb.timestamp.AbstractDbTimestampBoundStoreTest;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public class CassandraTimestampBoundStoreTest extends AbstractDbTimestampBoundStoreTest {
    public static final long CONFLICTING_BOUND = 3557616313682636848L;
    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final String ROW_AND_COLUMN_NAME = "ts";
    private static final Cell TIMESTAMP_BOUND_CELL =
            Cell.create(PtBytes.toBytes(ROW_AND_COLUMN_NAME), PtBytes.toBytes(ROW_AND_COLUMN_NAME));
    private static final long OFFSET = 100;
    private static final long SECOND_OFFSET = OFFSET + 1;


    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraTimestampIntegrationTest.class)
            .with(new CassandraContainer());

    private final CassandraKeyValueService kv = CassandraKeyValueService.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG);

    @Override
    public TimestampBoundStore createTimestampBoundStore() {
        return CassandraTimestampBoundStore.create(kv);
    }

    @Test
    public void storeWithEmptyTableThrows() {
        assertThatStoreUpperLimitThrows(SECOND_OFFSET);
    }

    @Test
    public void canGetNewFormat() {
        insertTimestampWithCorrectId(OFFSET);
        assertBoundInDbIsEqualTo(OFFSET);
    }

    @Test
    public void canGetOldFormat() {
        insertTimestampOld(OFFSET);
        assertBoundInDbIsEqualTo(OFFSET);
    }

    @Test
    public void storeWithWrongTimestampCorrectIdSucceeds() {
        long limit = store.getUpperLimit();
        insertTimestampWithCorrectId(limit + OFFSET);
        store.storeUpperLimit(limit + SECOND_OFFSET);
        assertBoundInDbIsEqualTo(limit + SECOND_OFFSET);
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
    public void storeWithRightTimestampNoIdSucceeds() {
        insertTimestampOld(OFFSET);
        assertThat(store.getUpperLimit()).isEqualTo(OFFSET);
        store.storeUpperLimit(SECOND_OFFSET);
        assertBoundInDbIsEqualTo(SECOND_OFFSET);
    }

    @Test
    public void storeWithRightTimestampWrongIdSucceeds() {
        insertTimestampWithFakeId(OFFSET);
        store.getUpperLimit();
        store.storeUpperLimit(SECOND_OFFSET);
        assertBoundInDbIsEqualTo(SECOND_OFFSET);
    }

    @Test
    public void checkFixForConversionCornerCase() {
        setTimestampTableValueTo(PtBytes.toBytes(CONFLICTING_BOUND));
        assertBoundInDbIsEqualTo(CONFLICTING_BOUND);
    }

    @After
    public void cleanUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    private void assertBoundInDbIsEqualTo(long limit) {
        assertThat(store.getUpperLimit()).isEqualTo(limit);
    }

    private void assertThatStoreUpperLimitThrows(long limit) {
        assertThatThrownBy(() -> store.storeUpperLimit(limit))
                .isExactlyInstanceOf(MultipleRunningTimestampServiceError.class);
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
        long id = ((CassandraTimestampBoundStore) store).getId();
        if (changeId) {
            id = id % 2 - 1;
        }
        setTimestampTableValueTo(PtBytes.toBytes(id + "_" + value));
    }

    private void setTimestampTableValueTo(byte[] data) {
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.put(AtlasDbConstants.TIMESTAMP_TABLE, ImmutableMap.of(TIMESTAMP_BOUND_CELL, data), CASSANDRA_TIMESTAMP);
    }
}
