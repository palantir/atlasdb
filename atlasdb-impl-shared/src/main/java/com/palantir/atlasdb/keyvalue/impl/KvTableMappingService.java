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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.commons.lang3.Validate;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.TableMappingService;
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
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.base.ClosableIterator;

public class KvTableMappingService implements TableMappingService {
    public static final TableMetadata NAMESPACE_TABLE_METADATA = TableMetadata.internal()
            .rowMetadata(NameMetadataDescription.create(ImmutableList.of(
                    NameComponentDescription.of("namespace", ValueType.VAR_STRING),
                    NameComponentDescription.of("table_name", ValueType.STRING))))
            .singleNamedColumn(AtlasDbConstants.NAMESPACE_SHORT_COLUMN_NAME, "short_name", ValueType.STRING)
            .build();

    protected final AtomicReference<BiMap<TableReference, TableReference>> tableMap = new AtomicReference<>();
    private final KeyValueService kvs;
    private final LongSupplier uniqueLongSupplier;
    private final ConcurrentHashMap<TableReference, Boolean> unmappedTables = new ConcurrentHashMap<>();

    protected KvTableMappingService(KeyValueService kvs, LongSupplier uniqueLongSupplier) {
        this.kvs = Preconditions.checkNotNull(kvs, "kvs must not be null");
        this.uniqueLongSupplier = Preconditions.checkNotNull(uniqueLongSupplier, "uniqueLongSupplier must not be null");
    }

    public static KvTableMappingService create(KeyValueService kvs, LongSupplier uniqueLongSupplier) {
        createNamespaceTable(kvs);
        KvTableMappingService ret = new KvTableMappingService(kvs, uniqueLongSupplier);
        ret.updateTableMap();
        return ret;
    }

    public static void createNamespaceTable(KeyValueService keyValueService) {
        keyValueService.createTable(AtlasDbConstants.NAMESPACE_TABLE, NAMESPACE_TABLE_METADATA.persistToBytes());
    }

    protected void updateTableMap() {
        updateTableMapWith(this::readTableMap);
    }

    @Override
    public TableReference addTable(TableReference tableRef) {
        if (tableRef.getNamespace().isEmptyNamespace()) {
            return tableRef;
        }

        TableReference existingShortName = tableMap.get().get(tableRef);
        if (existingShortName != null) {
            return existingShortName;
        }

        Cell key = getKeyCellForTable(tableRef);
        String shortName = AtlasDbConstants.NAMESPACE_PREFIX + uniqueLongSupplier.getAsLong();
        byte[] value = PtBytes.toBytes(shortName);
        try {
            kvs.putUnlessExists(AtlasDbConstants.NAMESPACE_TABLE, ImmutableMap.of(key, value));
        } catch (KeyAlreadyExistsException e) {
            return getAlreadyExistingMappedTableName(tableRef);
        }
        return TableReference.createWithEmptyNamespace(shortName);
    }

    private Cell getKeyCellForTable(TableReference tableRef) {
        return Cell.create(getBytesForTableRef(tableRef), AtlasDbConstants.NAMESPACE_SHORT_COLUMN_BYTES);
    }

    public static byte[] getBytesForTableRef(TableReference tableRef) {
        byte[] nameSpace = EncodingUtils.encodeVarString(tableRef.getNamespace().getName());
        byte[] table = PtBytes.toBytes(tableRef.getTablename());
        return Bytes.concat(nameSpace, table);
    }

    @Override
    public void removeTable(TableReference tableRef) {
        removeTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void removeTables(Set<TableReference> tableRefs) {
        if (kvs.getAllTableNames().contains(AtlasDbConstants.NAMESPACE_TABLE)) {
            Multimap<Cell, Long> deletionMap = tableRefs.stream()
                    .map(this::getKeyCellForTable)
                    .collect(Multimaps.toMultimap(
                            key -> key,
                            unused -> AtlasDbConstants.TRANSACTION_TS,
                            MultimapBuilder.hashKeys().arrayListValues()::build));
            kvs.delete(AtlasDbConstants.NAMESPACE_TABLE, deletionMap);
        }

        // Need to invalidate the table refs in case we end up re-creating the same table again.
        updateTableMapWith(() -> tableMapWithInvalidatedTableRefs(tableRefs));
    }

    private BiMap<TableReference, TableReference> tableMapWithInvalidatedTableRefs(Set<TableReference> tableRefs) {
        BiMap<TableReference, TableReference> newMap = HashBiMap.create(tableMap.get());
        tableRefs.forEach(newMap::remove);
        return newMap;
    }

    @Override
    public TableReference getMappedTableName(TableReference tableRef) throws TableMappingNotFoundException {
        if (tableRef.getNamespace().isEmptyNamespace()) {
            return tableRef;
        }

        TableReference shortTableName = getMappedTableRef(tableRef);
        validateShortName(tableRef, shortTableName);
        return shortTableName;
    }

    private TableReference getMappedTableRef(TableReference tableRef)
            throws TableMappingNotFoundException{
        TableReference candidate = tableMap.get().get(tableRef);
        if (candidate == null) {
            updateTableMap();
            candidate = tableMap.get().get(tableRef);
            if (candidate == null) {
                throw new TableMappingNotFoundException("Unable to resolve mapping for table reference " + tableRef);
            }
        }
        return candidate;
    }

    protected void validateShortName(TableReference tableRef, TableReference shortName) {
        Validate.isTrue(Schemas.isTableNameValid(shortName.getQualifiedName()),
                "Table mapper has an invalid table name for table reference %s: %s", tableRef, shortName);
    }


    @Override
    public <T> Map<TableReference, T> mapToShortTableNames(Map<TableReference, T> toMap)
            throws TableMappingNotFoundException {
        Map<TableReference, T> newMap = Maps.newHashMap();
        for (Map.Entry<TableReference, T> e : toMap.entrySet()) {
            newMap.put(getMappedTableName(e.getKey()), e.getValue());
        }
        return newMap;
    }

    @Override
    public Map<TableReference, TableReference> generateMapToFullTableNames(Set<TableReference> tableRefs) {
        Map<TableReference, TableReference> shortNameToFullTableName = Maps.newHashMapWithExpectedSize(
                tableRefs.size());
        Set<TableReference> tablesToReload = Sets.newHashSet();
        for (TableReference inputName : tableRefs) {
            if (inputName.isFullyQualifiedName()) {
                shortNameToFullTableName.put(inputName, inputName);
            } else {
                TableReference candidate = tableMap.get().inverse().get(inputName);
                if (candidate != null) {
                    shortNameToFullTableName.put(inputName, candidate);
                } else if (unmappedTables.containsKey(inputName)) {
                    shortNameToFullTableName.put(inputName, inputName);
                } else {
                    tablesToReload.add(inputName);
                }
            }
        }
        if (!tablesToReload.isEmpty()) {
            updateTableMap();
            Map<TableReference, TableReference> unmodifiableTableMap = ImmutableMap.copyOf(tableMap.get().inverse());
            for (TableReference tableRef : tablesToReload) {
                if (unmodifiableTableMap.containsKey(tableRef)) {
                    shortNameToFullTableName.put(tableRef, unmodifiableTableMap.get(tableRef));
                } else {
                    unmappedTables.put(tableRef, true);
                    shortNameToFullTableName.put(tableRef, tableRef);
                }
            }
        }
        return shortNameToFullTableName;
    }

    private void updateTableMapWith(Supplier<BiMap<TableReference, TableReference>> tableMapSupplier) {
        while (true) {
            BiMap<TableReference, TableReference> oldMap = tableMap.get();
            if (tableMap.compareAndSet(oldMap, tableMapSupplier.get())) {
                return;
            }
        }
    }

    protected BiMap<TableReference, TableReference> readTableMap() {
        BiMap<TableReference, TableReference> ret = HashBiMap.create();
        try (ClosableIterator<RowResult<Value>> range = rangeScanNamespaceTable()) {
            while (range.hasNext()) {
                RowResult<Value> row = range.next();
                String shortName = PtBytes.toString(row.getColumns()
                        .get(AtlasDbConstants.NAMESPACE_SHORT_COLUMN_BYTES).getContents());
                TableReference ref = getTableRefFromBytes(row.getRowName());
                ret.put(ref, TableReference.createWithEmptyNamespace(shortName));
            }
        }
        return ret;
    }

    private ClosableIterator<RowResult<Value>> rangeScanNamespaceTable() {
        return kvs.getRange(AtlasDbConstants.NAMESPACE_TABLE, RangeRequest.builder().build(), Long.MAX_VALUE);
    }

    public static TableReference getTableRefFromBytes(byte[] encodedTableRef) {
        String nameSpace = EncodingUtils.decodeVarString(encodedTableRef);
        int offset = EncodingUtils.sizeOfVarString(nameSpace);
        String tableName = PtBytes.toString(encodedTableRef, offset, encodedTableRef.length - offset);
        return TableReference.create(Namespace.create(nameSpace), tableName);
    }

    private TableReference getAlreadyExistingMappedTableName(TableReference tableRef) {
        try {
            return getMappedTableName(tableRef);
        } catch (TableMappingNotFoundException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

}
