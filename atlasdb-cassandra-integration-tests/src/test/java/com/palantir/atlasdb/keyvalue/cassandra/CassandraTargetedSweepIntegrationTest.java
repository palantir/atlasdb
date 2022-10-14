/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractTargetedSweepTest;
import com.palantir.atlasdb.table.description.TableMetadata;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraTargetedSweepIntegrationTest extends AbstractTargetedSweepTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    public CassandraTargetedSweepIntegrationTest() {
        super(CASSANDRA, CASSANDRA);
    }

    @Test
    public void targetedSweepIgnoresDroppedTablesWithMetadataPresent() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        kvs.createTable(TABLE_TO_BE_DROPPED, TableMetadata.allDefault().persistToBytes());

        put(TABLE_NAME, TEST_CELL, OLD_VALUE, 50);
        put(TABLE_NAME, TEST_CELL, NEW_VALUE, 150);
        put(TABLE_TO_BE_DROPPED, TEST_CELL, OLD_VALUE, 100);

        // uncommitted write to table to be dropped
        Cell uncommitted = Cell.create(PtBytes.toBytes("uncommitted"), PtBytes.toBytes("cell"));
        kvs.put(TABLE_TO_BE_DROPPED, ImmutableMap.of(uncommitted, PtBytes.toBytes(1L)), 120);
        sweepQueue.enqueue(
                ImmutableMap.of(TABLE_TO_BE_DROPPED, ImmutableMap.of(uncommitted, PtBytes.toBytes(1L))), 120);

        // this simulates failure to delete metadata after table was dropped in Cassandra
        kvs.dropTable(TABLE_TO_BE_DROPPED);
        kvs.put(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableMap.of(
                        CassandraKeyValueServices.getMetadataCell(TABLE_TO_BE_DROPPED),
                        TableMetadata.allDefault().persistToBytes()),
                System.currentTimeMillis());

        completeSweep(null, 90);
        assertThat(getValue(TABLE_NAME, 110)).isEqualTo(Value.create(PtBytes.toBytes(OLD_VALUE), 50));
        assertThat(getValue(TABLE_NAME, 160)).isEqualTo(Value.create(PtBytes.toBytes(NEW_VALUE), 150));

        completeSweep(null, 160);
        assertThat(getValue(TABLE_NAME, 110))
                .isEqualTo(Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP));
        assertThat(getValue(TABLE_NAME, 160)).isEqualTo(Value.create(PtBytes.toBytes(NEW_VALUE), 150));
    }
}
