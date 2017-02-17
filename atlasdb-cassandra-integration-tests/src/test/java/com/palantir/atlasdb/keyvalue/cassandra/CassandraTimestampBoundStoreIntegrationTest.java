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

import java.util.UUID;

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
import com.palantir.timestamp.AbstractTimestampBoundStoreWithId;
import com.palantir.timestamp.AbstractTimestampBoundStoreWithIdTest;

public class CassandraTimestampBoundStoreIntegrationTest extends AbstractTimestampBoundStoreWithIdTest {
    private static final Cell TIMESTAMP_BOUND_CELL = Cell.create(
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME),
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME));

    private final CassandraKeyValueService kv = CassandraKeyValueService.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG);

    @Override
    public AbstractTimestampBoundStoreWithId createTimestampBoundStore() {
        return CassandraTimestampBoundStore.create(kv);
    }

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraTimestampBoundStoreIntegrationTest.class)
            .with(new CassandraContainer());

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

    @Override
    protected void dropTimestampTable() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @Override
    protected void truncateTimestampTable() {
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @Override
    protected void closeKvs() {
        kv.close();
    }

    @Override
    protected void putEntryInTimestampTable(long value, UUID id) {
        setTimestampTableValueTo(CassandraTimestampBoundStoreEntry.getByteValueForBoundAndId(value, id));
    }

    private void setTimestampTableValueTo(byte[] data) {
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.put(AtlasDbConstants.TIMESTAMP_TABLE, ImmutableMap.of(TIMESTAMP_BOUND_CELL, data), CASSANDRA_TIMESTAMP);
    }

    @Override
    protected long getBoundFromDb() {
        Value value = kv.get(AtlasDbConstants.TIMESTAMP_TABLE, ImmutableMap.of(TIMESTAMP_BOUND_CELL, Long.MAX_VALUE))
                .get(TIMESTAMP_BOUND_CELL);
        return CassandraTimestampBoundStoreEntry.createFromBytes(value.getContents()).timestamp.get();
    }

    @Override
    protected UUID getIdFromDb() {
        Value value = kv.get(AtlasDbConstants.TIMESTAMP_TABLE, ImmutableMap.of(TIMESTAMP_BOUND_CELL, Long.MAX_VALUE))
                .get(TIMESTAMP_BOUND_CELL);
        return CassandraTimestampBoundStoreEntry.createFromBytes(value.getContents()).id.get();
    }
}
