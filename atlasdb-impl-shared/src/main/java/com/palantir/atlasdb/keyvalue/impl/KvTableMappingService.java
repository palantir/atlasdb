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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
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
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KvTableMappingService implements TableMappingService {
    private static final Logger log = LoggerFactory.getLogger(KvTableMappingService.class);

    public static final TableMetadata NAMESPACE_TABLE_METADATA = TableMetadata.internal()
            .rowMetadata(NameMetadataDescription.create(ImmutableList.of(
                    NameComponentDescription.of("namespace", ValueType.VAR_STRING),
                    NameComponentDescription.of("table_name", ValueType.STRING))))
            .singleNamedColumn(AtlasDbConstants.NAMESPACE_SHORT_COLUMN_NAME, "short_name", ValueType.STRING)
            .build();

    protected final AtomicReference<BiMap<TableReference, TableReference>> tableMap = new AtomicReference<>();
    private final KeyValueService kvs;
    private final LongSupplier uniqueLongSupplier;
    private final Pattern namespaceValidation;
    private final Set<TableReference> unmappedTables = Collections.newSetFromMap(new ConcurrentHashMap<>());

    protected KvTableMappingService(KeyValueService kvs, LongSupplier uniqueLongSupplier, Pattern namespaceValidation) {
        this.kvs = Preconditions.checkNotNull(kvs, "kvs must not be null");
        this.uniqueLongSupplier = Preconditions.checkNotNull(uniqueLongSupplier, "uniqueLongSupplier must not be null");
        this.namespaceValidation = namespaceValidation;
    }

    public static KvTableMappingService create(KeyValueService kvs, LongSupplier uniqueLongSupplier) {
        return createWithCustomNamespaceValidation(kvs, uniqueLongSupplier, Namespace.STRICTLY_CHECKED_NAME);
    }

    public static KvTableMappingService createWithCustomNamespaceValidation(
            KeyValueService kvs, LongSupplier uniqueLongSupplier, Pattern namespaceValidation) {
        createNamespaceTable(kvs);
        KvTableMappingService ret = new KvTableMappingService(kvs, uniqueLongSupplier, namespaceValidation);
        ret.updateTableMap();
        return ret;
    }

    public static void createNamespaceTable(KeyValueService keyValueService) {
        keyValueService.createTable(AtlasDbConstants.NAMESPACE_TABLE, NAMESPACE_TABLE_METADATA.persistToBytes());
    }

    protected void updateTableMap() {
        tableMap.updateAndGet(toBeReplaced -> readTableMap());
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

    static Cell getKeyCellForTable(TableReference tableRef) {
        return Cell.create(getBytesForTableRef(tableRef), AtlasDbConstants.NAMESPACE_SHORT_COLUMN_BYTES);
    }

    public static byte[] getBytesForTableRef(TableReference tableRef) {
        byte[] nameSpace = EncodingUtils.encodeVarString(tableRef.getNamespace().getName());
        byte[] table = PtBytes.toBytes(tableRef.getTablename());
        return Bytes.concat(nameSpace, table);
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

    /**
     * This method guarantees atomicity, in that the table mapping service will atomically be updated to reflect the
     * removal of all the tables in tableRefs.
     */
    @Override
    public void removeTables(Set<TableReference> tableRefs) {
        if (kvs.getAllTableNames().contains(AtlasDbConstants.NAMESPACE_TABLE)) {
            Multimap<Cell, Long> deletionMap = tableRefs.stream()
                    .map(KvTableMappingService::getKeyCellForTable)
                    .collect(Multimaps.toMultimap(
                            key -> key,
                            unused -> AtlasDbConstants.TRANSACTION_TS,
                            MultimapBuilder.hashKeys().arrayListValues()::build));
            kvs.delete(AtlasDbConstants.NAMESPACE_TABLE, deletionMap);
        }

        tableMap.updateAndGet(oldTableMap -> invalidateTables(oldTableMap, tableRefs));
    }

    private static BiMap<TableReference, TableReference> invalidateTables(
            Map<TableReference, TableReference> oldMap, Set<TableReference> tableRefs) {
        BiMap<TableReference, TableReference> newMap = HashBiMap.create(oldMap);
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

    private TableReference getMappedTableRef(TableReference tableRef) throws TableMappingNotFoundException {
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
        Validate.isTrue(
                Schemas.isTableNameValid(shortName.getQualifiedName()),
                "Table mapper has an invalid table name for table reference %s: %s",
                tableRef,
                shortName);
    }

    @Override
    public <T> Map<TableReference, T> mapToShortTableNames(Map<TableReference, T> toMap)
            throws TableMappingNotFoundException {
        Map<TableReference, T> newMap = new HashMap<>();
        for (Map.Entry<TableReference, T> e : toMap.entrySet()) {
            newMap.put(getMappedTableName(e.getKey()), e.getValue());
        }
        return newMap;
    }

    @Override
    public Map<TableReference, TableReference> generateMapToFullTableNames(Set<TableReference> tableRefs) {
        Map<TableReference, TableReference> shortToFullTableName = Maps.newHashMapWithExpectedSize(tableRefs.size());
        Set<TableReference> tablesToReload = new HashSet<>();
        Map<TableReference, TableReference> reverseTableMapSnapshot =
                ImmutableMap.copyOf(tableMap.get().inverse());

        for (TableReference inputName : tableRefs) {
            if (inputName.isFullyQualifiedName()) {
                shortToFullTableName.put(inputName, inputName);
            } else if (reverseTableMapSnapshot.containsKey(inputName)) {
                shortToFullTableName.put(inputName, reverseTableMapSnapshot.get(inputName));
            } else if (unmappedTables.contains(inputName)) {
                shortToFullTableName.put(inputName, inputName);
            } else {
                tablesToReload.add(inputName);
            }
        }

        if (!tablesToReload.isEmpty()) {
            updateTableMap();
            Map<TableReference, TableReference> unmodifiableTableMap =
                    ImmutableMap.copyOf(tableMap.get().inverse());
            for (TableReference tableRef : tablesToReload) {
                if (unmodifiableTableMap.containsKey(tableRef)) {
                    shortToFullTableName.put(tableRef, unmodifiableTableMap.get(tableRef));
                } else {
                    unmappedTables.add(tableRef);
                    shortToFullTableName.put(tableRef, tableRef);
                }
            }
        }

        return shortToFullTableName;
    }

    protected BiMap<TableReference, TableReference> readTableMap() {
        // TODO (jkong) Remove after PDS-117310 is resolved.
        if (log.isTraceEnabled()) {
            log.trace(
                    "Attempting to read the table mapping from the namespace table.",
                    new SafeRuntimeException("I exist to show you the stack trace"));
        }
        BiMap<TableReference, TableReference> ret = HashBiMap.create();
        try (ClosableIterator<RowResult<Value>> range = rangeScanNamespaceTable()) {
            while (range.hasNext()) {
                RowResult<Value> row = range.next();
                String shortName = PtBytes.toString(row.getColumns()
                        .get(AtlasDbConstants.NAMESPACE_SHORT_COLUMN_BYTES)
                        .getContents());
                TableReference ref = getTableRefFromBytes(row.getRowName());
                ret.put(ref, TableReference.createWithEmptyNamespace(shortName));
            }
        }
        // TODO (jkong) Remove after PDS-117310 is resolved.
        if (log.isTraceEnabled()) {
            log.trace(
                    "Successfully read {} entries from the namespace table, to refresh the table map.",
                    SafeArg.of("entriesRead", ret.size()),
                    new SafeRuntimeException("I exist to show you the stack trace"));
        }
        return ret;
    }

    private ClosableIterator<RowResult<Value>> rangeScanNamespaceTable() {
        return kvs.getRange(
                AtlasDbConstants.NAMESPACE_TABLE, RangeRequest.builder().build(), Long.MAX_VALUE);
    }

    private TableReference getTableRefFromBytes(byte[] encodedTableRef) {
        String nameSpace = EncodingUtils.decodeVarString(encodedTableRef);
        int offset = EncodingUtils.sizeOfVarString(nameSpace);
        String tableName = PtBytes.toString(encodedTableRef, offset, encodedTableRef.length - offset);
        return TableReference.create(Namespace.create(nameSpace, namespaceValidation), tableName);
    }
}
