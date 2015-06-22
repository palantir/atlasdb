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

package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.primitives.Bytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.schema.TableReference;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.ClosableIterator;

public class KVTableMappingService extends AbstractTableMappingService {
    public static final String SHORT_NAME_COLUMN_NAME = "s";
    private static final byte[] SHORT_NAME_COLUMN_BYTES = PtBytes.toBytes(SHORT_NAME_COLUMN_NAME);
    public static final TableMetadata NAMESPACE_TABLE_METADATA = new TableMetadata(
        new NameMetadataDescription(ImmutableList.of(
                new NameComponentDescription("namespace", ValueType.VAR_STRING),
                new NameComponentDescription("table_name", ValueType.STRING))),
        new ColumnMetadataDescription(ImmutableList.of(
            new NamedColumnDescription(SHORT_NAME_COLUMN_NAME, "short_name", ColumnValueDescription.forType(ValueType.STRING)))),
        ConflictHandler.IGNORE_ALL);

    private final KeyValueService kv;
    private final Supplier<Long> uniqueLongSupplier;

    private KVTableMappingService(KeyValueService kv, Supplier<Long> uniqueLongSupplier) {
        this.kv = Preconditions.checkNotNull(kv);
        this.uniqueLongSupplier = Preconditions.checkNotNull(uniqueLongSupplier);
    }

    public static KVTableMappingService create(KeyValueService kvs, Supplier<Long> uniqueLongSupplier) {
        createTables(kvs);
        KVTableMappingService ret = new KVTableMappingService(kvs, uniqueLongSupplier);
        ret.updateTableMap();
        return ret;
    }

    public static void createTables(KeyValueService keyValueService) {
        keyValueService.createTable(AtlasDbConstants.NAMESPACE_TABLE, Integer.MAX_VALUE);
        keyValueService.putMetadataForTable(AtlasDbConstants.NAMESPACE_TABLE, NAMESPACE_TABLE_METADATA.persistToBytes());
    }

    public static final byte[] getBytesForTableRef(TableReference tableRef) {
        byte[] nameSpace = EncodingUtils.encodeVarString(tableRef.getNamespace().getName());
        byte[] table = PtBytes.toBytes(tableRef.getTablename());
        return Bytes.concat(nameSpace, table);
    }

    public static final TableReference getTableRefFromBytes(byte[] encodedTableRef) {
        String nameSpace = EncodingUtils.decodeVarString(encodedTableRef);
        int offset = EncodingUtils.sizeOfVarString(nameSpace);
        String tableName = PtBytes.toString(encodedTableRef, offset, encodedTableRef.length - offset);
        return TableReference.create(Namespace.create(nameSpace), tableName);
    }

    @Override
    public String addTable(TableReference tableRef) {
        if (tableRef.getNamespace() == Namespace.EMPTY_NAMESPACE) {
            return tableRef.getTablename();
        }
        if (tableMap.get().containsKey(tableRef)) {
            return tableMap.get().get(tableRef);
        }
        Cell key = Cell.create(getBytesForTableRef(tableRef), SHORT_NAME_COLUMN_BYTES);
        String shortName = AtlasDbConstants.NAMESPACE_PREFIX + Preconditions.checkNotNull(uniqueLongSupplier.get());
        byte[] value = PtBytes.toBytes(shortName);
        try {
            kv.putUnlessExists(AtlasDbConstants.NAMESPACE_TABLE, ImmutableMap.of(key, value));
        } catch (KeyAlreadyExistsException e) {
            return getShortTableName(tableRef);
        }
        return shortName;
    }

    @Override
    public void removeTable(TableReference tableRef) {
        Cell key = Cell.create(getBytesForTableRef(tableRef), SHORT_NAME_COLUMN_BYTES);
        kv.delete(AtlasDbConstants.NAMESPACE_TABLE, ImmutableMultimap.of(key, 0L));
        // Need to invalidate the table ref in case we end up re-creating the same table
        // again. Frequently when we drop one table we end up dropping a bunch of tables,
        // so just invalidate everything.
        tableMap.set(HashBiMap.<TableReference, String>create());
    }

    @Override
    protected BiMap<TableReference, String> readTableMap() {
        BiMap<TableReference, String> ret = HashBiMap.create();
        ClosableIterator<RowResult<Value>> range = kv.getRange(AtlasDbConstants.NAMESPACE_TABLE, RangeRequest.builder().build(), Long.MAX_VALUE);
        try {
            while (range.hasNext()) {
                RowResult<Value> row = range.next();
                String shortName = PtBytes.toString(row.getColumns().get(SHORT_NAME_COLUMN_BYTES).getContents());
                TableReference ref = getTableRefFromBytes(row.getRowName());
                ret.put(ref, shortName);
            }
        } finally {
            range.close();
        }
        return ret;
    }

    @Override
    public boolean isInitializedNamespace(Namespace namespace) {
        for (TableReference ref : tableMap.get().keySet()) {
            if (ref.getNamespace().equals(namespace)) {
                return true;
            }
        }
        updateTableMap();
        for (TableReference ref : tableMap.get().keySet()) {
            if (ref.getNamespace().equals(namespace)) {
                return true;
            }
        }
        return false;
    }
}
