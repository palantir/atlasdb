// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.schema;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.ptobject.EncodingUtils.EncodingType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

/**
 * This checkpointer creates a temporary table for checkpointing. If you are writing an upgrade
 * task, use {@link UpgradeTaskCheckpointer} instead.
 */
public class GeneralTaskCheckpointer extends AbstractTaskCheckpointer {
    private static final String SHORT_COLUMN_NAME = "s";

    private final String checkpointTable;
    private final KeyValueService kvs;

    public GeneralTaskCheckpointer(String checkpointTable,
                                   KeyValueService kvs,
                                   TransactionManager txManager) {
        super(txManager);
        this.checkpointTable = checkpointTable;
        this.kvs = kvs;
    }

    @Override
    public void checkpoint(String extraId, long rangeId, byte[] nextRowName, Transaction t) {
        Cell cell = getCell(extraId, rangeId);
        Map<Cell, byte[]> values = ImmutableMap.of(cell, toDb(nextRowName, false));
        t.put(checkpointTable, values);
    }

    @Override
    public byte[] getCheckpoint(String extraId, long rangeId, Transaction t) {
        Cell cell = getCell(extraId, rangeId);
        byte[] value = t.get(checkpointTable, ImmutableSet.of(cell)).get(cell);
        return fromDb(value);
    }

    @Override
    public void createCheckpoints(final String extraId,
                                  final Map<Long, byte[]> startById) {
        getSchema().createTable(kvs, checkpointTable);

        txManager.runTaskWithRetry(new TransactionTask<Map<Long, byte[]>, RuntimeException>() {
            @Override
            public Map<Long, byte[]> execute(Transaction t) {
                Set<byte[]> rows = Sets.newHashSet();
                for (long rangeId : startById.keySet()) {
                    rows.add(getRowName(extraId, rangeId));
                }

                Map<byte[], RowResult<byte[]>> rr = t.getRows(
                        checkpointTable,
                        rows,
                        ColumnSelection.all());

                if (rr.isEmpty()) {
                    Map<Cell, byte[]> values = Maps.newHashMap();
                    for (Entry<Long, byte[]> e : startById.entrySet()) {
                        Cell cell = getCell(extraId, e.getKey());
                        byte[] value = toDb(e.getValue(), true);
                        values.put(cell, value);
                    }
                    t.put(checkpointTable, values);
                }
                return null;
            }
        });
    }

    @Override
    public void deleteCheckpoints() {
        getSchema().deleteTable(kvs, checkpointTable);
    }

    private Cell getCell(String extraId, long rangeId) {
        byte[] rowName = getRowName(extraId, rangeId);
        byte[] columnName = PtBytes.toBytes(SHORT_COLUMN_NAME);
        return Cell.create(rowName, columnName);
    }

    private byte[] getRowName(String extraId, long rangeId) {
        List<EncodingType> types = ImmutableList.of(
                new EncodingType(ValueType.VAR_STRING),
                new EncodingType(ValueType.VAR_LONG));
        List<Object> components = ImmutableList.<Object>of(
                extraId,
                rangeId);
        return EncodingUtils.toBytes(types, components);
    }

    private Schema getSchema() {
        Schema schema = new Schema();
        schema.addTableDefinition(checkpointTable, new TableDefinition() {{
            rowName();
                rowComponent("table_name", ValueType.VAR_STRING);
                rowComponent("range_id",   ValueType.VAR_LONG);
            columns();
                column("start", SHORT_COLUMN_NAME, ValueType.BLOB);
            rangeScanAllowed();
            conflictHandler(ConflictHandler.IGNORE_ALL);
        }});
        return schema;
    }
}
