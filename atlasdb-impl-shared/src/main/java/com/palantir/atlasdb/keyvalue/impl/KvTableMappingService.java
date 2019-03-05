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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.Bytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.base.ClosableIterator;

public class KvTableMappingService extends AbstractTableMappingService {
    public static final TableMetadata NAMESPACE_TABLE_METADATA = TableMetadata.internal()
            .rowMetadata(NameMetadataDescription.create(ImmutableList.of(
                    NameComponentDescription.of("namespace", ValueType.VAR_STRING),
                    NameComponentDescription.of("table_name", ValueType.STRING))))
            .singleNamedColumn(AtlasDbConstants.NAMESPACE_SHORT_COLUMN_NAME, "short_name", ValueType.STRING)
            .build();

    private final KeyValueService kv;
    private final Supplier<Long> uniqueLongSupplier;

    protected KvTableMappingService(KeyValueService kv, Supplier<Long> uniqueLongSupplier) {
        this.kv = Preconditions.checkNotNull(kv, "kv must not be null");
        this.uniqueLongSupplier = Preconditions.checkNotNull(uniqueLongSupplier, "uniqueLongSupplier must not be null");
    }

    public static KvTableMappingService create(KeyValueService kvs, Supplier<Long> uniqueLongSupplier) {
        createTables(kvs);
        KvTableMappingService ret = new KvTableMappingService(kvs, uniqueLongSupplier);
        ret.updateTableMap();
        return ret;
    }

    public static void createTables(KeyValueService keyValueService) {
        keyValueService.createTable(AtlasDbConstants.NAMESPACE_TABLE, NAMESPACE_TABLE_METADATA.persistToBytes());
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
    public TableReference addTable(TableReference tableRef) {
        if (tableRef.getNamespace().isEmptyNamespace()) {
            return tableRef;
        }
        if (tableMap.get().containsKey(tableRef)) {
            return tableMap.get().get(tableRef);
        }
        Cell key = Cell.create(getBytesForTableRef(tableRef), AtlasDbConstants.NAMESPACE_SHORT_COLUMN_BYTES);
        String shortName = AtlasDbConstants.NAMESPACE_PREFIX
                + Preconditions.checkNotNull(uniqueLongSupplier.get(), "uniqueLongSupplier returned null");
        byte[] value = PtBytes.toBytes(shortName);
        try {
            kv.putUnlessExists(AtlasDbConstants.NAMESPACE_TABLE, ImmutableMap.of(key, value));
        } catch (KeyAlreadyExistsException e) {
            return getAlreadyExistingMappedTableName(tableRef);
        }
        return TableReference.createWithEmptyNamespace(shortName);
    }

    private TableReference getAlreadyExistingMappedTableName(TableReference tableRef) {
        try {
            return getMappedTableName(tableRef);
        } catch (TableMappingNotFoundException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @Override
    public void removeTable(TableReference tableRef) {
        removeTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void removeTables(Set<TableReference> tableRefs) {
        if (kv.getAllTableNames().contains(AtlasDbConstants.NAMESPACE_TABLE)) {
            Multimap<Cell, Long> deletionMap = tableRefs.stream()
                    .map(tableRef -> Cell.create(getBytesForTableRef(tableRef),
                            AtlasDbConstants.NAMESPACE_SHORT_COLUMN_BYTES))
                    .collect(Multimaps.toMultimap(
                            key -> key,
                            unused -> AtlasDbConstants.TRANSACTION_TS,
                            MultimapBuilder.hashKeys().arrayListValues()::build));
            kv.delete(AtlasDbConstants.NAMESPACE_TABLE, deletionMap);
        }

        // Need to invalidate the table refs in case we end up re-creating the same table again.
        while (true) {
            BiMap<TableReference, TableReference> existingMap = tableMap.get();
            BiMap<TableReference, TableReference> newMap = HashBiMap.create(existingMap);
            tableRefs.forEach(newMap::remove);
            if (tableMap.compareAndSet(existingMap, newMap)) {
                return;
            }
        }
    }

    @Override
    protected BiMap<TableReference, TableReference> readTableMap() {
        BiMap<TableReference, TableReference> ret = HashBiMap.create();
        ClosableIterator<RowResult<Value>> range = kv.getRange(
                AtlasDbConstants.NAMESPACE_TABLE,
                RangeRequest.builder().build(),
                Long.MAX_VALUE);
        try {
            while (range.hasNext()) {
                RowResult<Value> row = range.next();
                String shortName = PtBytes.toString(row.getColumns()
                        .get(AtlasDbConstants.NAMESPACE_SHORT_COLUMN_BYTES).getContents());
                TableReference ref = getTableRefFromBytes(row.getRowName());
                ret.put(ref, TableReference.createWithEmptyNamespace(shortName));
            }
        } finally {
            range.close();
        }
        return ret;
    }

}
