package com.palantir.atlasdb.table.description.generated;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.generation.ColumnValues;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.persist.Persistables;
import java.lang.Iterable;
import java.lang.Long;
import java.lang.String;
import java.lang.SuppressWarnings;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Generated;

@Generated("com.palantir.atlasdb.table.description.render.TableRendererV2")
@SuppressWarnings("all")
public final class SchemaApiTestV2Table {
    private static final String rawTableName = "SchemaApiTest";

    private final Transaction t;

    private final TableReference tableRef;

    private SchemaApiTestV2Table(Transaction t, Namespace namespace) {
        this.tableRef = TableReference.create(namespace, rawTableName);
        this.t = t;
    }

    public static SchemaApiTestV2Table of(Transaction t, Namespace namespace) {
        return new SchemaApiTestV2Table(t, namespace);
    }

    public static String getRawTableName() {
        return rawTableName;
    }

    public TableReference getTableRef() {
        return tableRef;
    }

    public String getTableName() {
        return tableRef.getQualifiedName();
    }

    public Namespace getNamespace() {
        return tableRef.getNamespace();
    }

    /**
     * Returns the value for column Column1 and specified row components. */
    public Optional<Long> getColumn1(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] bytes = row.persistToBytes();
        ColumnSelection colSelection = 
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), colSelection).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        }
        else {
            return Optional.of(SchemaApiTestTable.SchemaApiTestRowResult.of(rowResult).getColumn1());
        }
    }

    /**
     * Returns a mapping from row keys to value at column Column1. Ordering in the
     * map is not guaranteed. As the values are all loaded in memory, do not use
     * for large amounts of data. If the column does not exist for a key, the entry
     * will be omitted from the map. */
    public Map<String, Long> getColumn1(Iterable<String> rowKeys) {
        ColumnSelection colSelection = 
                 ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        List<SchemaApiTestTable.SchemaApiTestRow> rows = Lists
                .newArrayList(rowKeys)
                .stream()
                .map(SchemaApiTestTable.SchemaApiTestRow::of)
                .collect(Collectors.toList());
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), colSelection);
        return results
                .values()
                .stream()
                .map(entry -> SchemaApiTestTable.SchemaApiTestRowResult.of(entry))
                .collect(Collectors.toMap(
                     entry -> entry.getRowName().getComponent1(), 
                     SchemaApiTestTable.SchemaApiTestRowResult::getColumn1));
    }

    /**
     * Returns a mapping from all the row keys to their value at column Column1 (if that column exists).
     * Ordering in the map is not guaranteed. As the values are all loaded in memory, do not use
     * for large amounts of data.  */
    public Map<String, Long> getAllColumn1() {
        ColumnSelection colSelection = 
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        return getRowRangeColumn1(RangeRequest.all());
    }

    /**
     * Returns a mapping from all the row keys in a rangeRequest to their value at column Column1
     * (if that column exists). Ordering in the map is not guaranteed. As the values are all
     * loaded in memory, do not use for large amounts of data.  */
    public Map<String, Long> getRowRangeColumn1(RangeRequest rangeRequest) {
        ColumnSelection colSelection = 
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        rangeRequest = rangeRequest.getBuilder().retainColumns(colSelection).build();
        Preconditions.checkArgument(rangeRequest.getColumnNames().size() <= 1,
                "Must not request columns other than Column1.");
        return BatchingVisitableView.of(t.getRange(tableRef, rangeRequest))
                .immutableCopy()
                .stream()
                .map(entry -> SchemaApiTestTable.SchemaApiTestRowResult.of(entry))
                .collect(Collectors.toMap(
                     entry -> entry.getRowName().getComponent1(), 
                     SchemaApiTestTable.SchemaApiTestRowResult::getColumn1));
    }

    /**
     * Returns the value for column Column2 and specified row components. */
    public Optional<String> getColumn2(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] bytes = row.persistToBytes();
        ColumnSelection colSelection = 
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("d")));
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), colSelection).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        }
        else {
            return Optional.of(SchemaApiTestTable.SchemaApiTestRowResult.of(rowResult).getColumn2());
        }
    }

    /**
     * Returns a mapping from row keys to value at column Column2. Ordering in the
     * map is not guaranteed. As the values are all loaded in memory, do not use
     * for large amounts of data. If the column does not exist for a key, the entry
     * will be omitted from the map. */
    public Map<String, String> getColumn2(Iterable<String> rowKeys) {
        ColumnSelection colSelection = 
                 ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("d")));
        List<SchemaApiTestTable.SchemaApiTestRow> rows = Lists
                .newArrayList(rowKeys)
                .stream()
                .map(SchemaApiTestTable.SchemaApiTestRow::of)
                .collect(Collectors.toList());
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), colSelection);
        return results
                .values()
                .stream()
                .map(entry -> SchemaApiTestTable.SchemaApiTestRowResult.of(entry))
                .collect(Collectors.toMap(
                     entry -> entry.getRowName().getComponent1(), 
                     SchemaApiTestTable.SchemaApiTestRowResult::getColumn2));
    }

    /**
     * Returns a mapping from all the row keys to their value at column Column2 (if that column exists).
     * Ordering in the map is not guaranteed. As the values are all loaded in memory, do not use
     * for large amounts of data.  */
    public Map<String, String> getAllColumn2() {
        ColumnSelection colSelection = 
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("d")));
        return getRowRangeColumn2(RangeRequest.all());
    }

    /**
     * Returns a mapping from all the row keys in a rangeRequest to their value at column Column2
     * (if that column exists). Ordering in the map is not guaranteed. As the values are all
     * loaded in memory, do not use for large amounts of data.  */
    public Map<String, String> getRowRangeColumn2(RangeRequest rangeRequest) {
        ColumnSelection colSelection = 
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("d")));
        rangeRequest = rangeRequest.getBuilder().retainColumns(colSelection).build();
        Preconditions.checkArgument(rangeRequest.getColumnNames().size() <= 1,
                "Must not request columns other than Column2.");
        return BatchingVisitableView.of(t.getRange(tableRef, rangeRequest))
                .immutableCopy()
                .stream()
                .map(entry -> SchemaApiTestTable.SchemaApiTestRowResult.of(entry))
                .collect(Collectors.toMap(
                     entry -> entry.getRowName().getComponent1(), 
                     SchemaApiTestTable.SchemaApiTestRowResult::getColumn2));
    }

    /**
     * Delete all columns for specified row components. */
    public void deleteRow(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] rowBytes = row.persistToBytes();
        Set<Cell> cells = new HashSet<>();
        cells.add(Cell.create(rowBytes, PtBytes.toCachedBytes("c")));
        cells.add(Cell.create(rowBytes, PtBytes.toCachedBytes("d")));
        t.delete(tableRef, cells);
    }

    /**
     * Delete the value at column Column1 (if it exists) for the specified row-key. */
    public void deleteColumn1(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] rowBytes = row.persistToBytes();
        Set<Cell> cells = ImmutableSet.of(Cell.create(rowBytes, PtBytes.toCachedBytes("c")));
        t.delete(tableRef, cells);
    }

    /**
     * Delete the value at column Column2 (if it exists) for the specified row-key. */
    public void deleteColumn2(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] rowBytes = row.persistToBytes();
        Set<Cell> cells = ImmutableSet.of(Cell.create(rowBytes, PtBytes.toCachedBytes("d")));
        t.delete(tableRef, cells);
    }

    /**
     * Takes the row-keys and a value to be inserted at column Column1. */
    public void putColumn1(String component1, Long column1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        t.put(tableRef, ColumnValues.toCellValues(ImmutableMultimap.of(row, SchemaApiTestTable.Column1.of(column1))));
    }

    /**
     * Takes a function that would update the value at column Column1, for the specified row
     * components. No effect if there is no value at that column. */
    public void updateColumn1(String component1, Function<Long, Long> processor) {
        Optional<Long> result = getColumn1(component1);
        if (result.isPresent()) {
            Long newValue = processor.apply(result.get());
            if (newValue != result.get()) {
                putColumn1(component1, processor.apply(result.get()));
            }
        }
    }

    /**
     * Takes the row-keys and a value to be inserted at column Column2. */
    public void putColumn2(String component1, String column2) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        t.put(tableRef, ColumnValues.toCellValues(ImmutableMultimap.of(row, SchemaApiTestTable.Column2.of(column2))));
    }

    /**
     * Takes a function that would update the value at column Column2, for the specified row
     * components. No effect if there is no value at that column. */
    public void updateColumn2(String component1, Function<String, String> processor) {
        Optional<String> result = getColumn2(component1);
        if (result.isPresent()) {
            String newValue = processor.apply(result.get());
            if (newValue != result.get()) {
                putColumn2(component1, processor.apply(result.get()));
            }
        }
    }
}
