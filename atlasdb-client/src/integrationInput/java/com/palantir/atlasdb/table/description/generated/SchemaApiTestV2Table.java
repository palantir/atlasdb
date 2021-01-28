package com.palantir.atlasdb.table.description.generated;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.test.StringValue;
import com.palantir.atlasdb.table.generation.ColumnValues;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.persist.Persistables;
import java.lang.Iterable;
import java.lang.Long;
import java.lang.String;
import java.lang.SuppressWarnings;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Generated;

@Generated("com.palantir.atlasdb.table.description.render.TableRendererV2")
@SuppressWarnings({
        "all",
        "deprecation"
})
public class SchemaApiTestV2Table {
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
     * Returns the value for column Column1 and specified row components.
     */
    public Optional<Long> getColumn1(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] bytes = row.persistToBytes();
        ColumnSelection colSelection = 
                ColumnSelection.create(ImmutableList.of(PtBytes.toCachedBytes("c")));

        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), colSelection).get(bytes);
        if (rowResult == null) {
             return Optional.empty();
        } else {
             return Optional.of(SchemaApiTestTable.SchemaApiTestRowResult.of(rowResult).getColumn1());
        }
    }

    /**
     * Returns a mapping from the specified row keys to their value at column Column1.
     * As the Column1 values are all loaded in memory, do not use for large amounts of data.
     * If the column does not exist for a key, the entry will be omitted from the map.
     */
    public Map<String, Long> getColumn1(Iterable<String> rowKeys) {
        ColumnSelection colSelection = 
                 ColumnSelection.create(ImmutableList.of(PtBytes.toCachedBytes("c")));
        List<SchemaApiTestTable.SchemaApiTestRow> rows = Lists
                .newArrayList(rowKeys)
                .stream()
                .map(SchemaApiTestTable.SchemaApiTestRow::of)
                .collect(Collectors.toList());

        NavigableMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), colSelection);
        return results
                .values()
                .stream()
                .map(entry -> SchemaApiTestTable.SchemaApiTestRowResult.of(entry))
                .collect(Collectors.toMap(
                     entry -> entry.getRowName().getComponent1(), 
                     SchemaApiTestTable.SchemaApiTestRowResult::getColumn1));
    }

    /**
     * Returns a mapping from all the row keys in a rangeRequest to their value at column Column1
     * (if that column exists for the row-key). As the Column1 values are all loaded in memory,
     * do not use for large amounts of data. The order of results is preserved in the map.
     */
    public LinkedHashMap<String, Long> getSmallRowRangeColumn1(RangeRequest rangeRequest) {
        ColumnSelection colSelection =
                ColumnSelection.create(ImmutableList.of(PtBytes.toCachedBytes("c")));
        rangeRequest = rangeRequest.getBuilder().retainColumns(colSelection).build();
        Preconditions.checkArgument(rangeRequest.getColumnNames().size() <= 1,
                "Must not request columns other than Column1.");

        LinkedHashMap<String, Long> resultsMap = new LinkedHashMap<>();
        BatchingVisitableView.of(t.getRange(tableRef, rangeRequest))
                .immutableCopy().forEach(entry -> {
                     SchemaApiTestTable.SchemaApiTestRowResult resultEntry =
                          SchemaApiTestTable.SchemaApiTestRowResult.of(entry);
                     resultsMap.put(resultEntry.getRowName().getComponent1(), resultEntry.getColumn1());
                });
        return resultsMap;
    }

    /**
     * Returns a mapping from all the row keys in a range to their value at column Column1
     * (if that column exists for the row-key). As the Column1 values are all loaded in memory,
     * do not use for large amounts of data. The order of results is preserved in the map.
     */
    public LinkedHashMap<String, Long> getSmallRowRangeColumn1(String startInclusive,
            String endExclusive) {
        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(SchemaApiTestTable.SchemaApiTestRow.of(startInclusive).persistToBytes())
                .endRowExclusive(SchemaApiTestTable.SchemaApiTestRow.of(endExclusive).persistToBytes())
                .build();
        return getSmallRowRangeColumn1(rangeRequest);
    }

    /**
     * Returns a mapping from the first sizeLimit row keys in a rangeRequest to their value
     * at column Column1 (if that column exists). As the Column1 entries are all loaded in memory,
     * do not use for large values of sizeLimit. The order of results is preserved in the map.
     */
    public LinkedHashMap<String, Long> getSmallRowRangeColumn1(RangeRequest rangeRequest,
            int sizeLimit) {
        ColumnSelection colSelection =
                ColumnSelection.create(ImmutableList.of(PtBytes.toCachedBytes("c")));
        rangeRequest = rangeRequest.getBuilder().retainColumns(colSelection).batchHint(sizeLimit).build();
        Preconditions.checkArgument(rangeRequest.getColumnNames().size() <= 1,
                "Must not request columns other than Column1.");

        LinkedHashMap<String, Long> resultsMap = new LinkedHashMap<>();
        BatchingVisitableView.of(t.getRange(tableRef, rangeRequest))
                .batchAccept(sizeLimit, batch -> {
                     batch.forEach(entry -> {
                         SchemaApiTestTable.SchemaApiTestRowResult resultEntry =
                              SchemaApiTestTable.SchemaApiTestRowResult.of(entry);
                         resultsMap.put(resultEntry.getRowName().getComponent1(), resultEntry.getColumn1());
                     });
                     return false; // stops the traversal after the first batch
                });
        return resultsMap;
    }

    /**
     * Returns the value for column Column2 and specified row components.
     */
    public Optional<StringValue> getColumn2(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] bytes = row.persistToBytes();
        ColumnSelection colSelection = 
                ColumnSelection.create(ImmutableList.of(PtBytes.toCachedBytes("d")));

        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), colSelection).get(bytes);
        if (rowResult == null) {
             return Optional.empty();
        } else {
             return Optional.of(SchemaApiTestTable.SchemaApiTestRowResult.of(rowResult).getColumn2());
        }
    }

    /**
     * Returns a mapping from the specified row keys to their value at column Column2.
     * As the Column2 values are all loaded in memory, do not use for large amounts of data.
     * If the column does not exist for a key, the entry will be omitted from the map.
     */
    public Map<String, StringValue> getColumn2(Iterable<String> rowKeys) {
        ColumnSelection colSelection = 
                 ColumnSelection.create(ImmutableList.of(PtBytes.toCachedBytes("d")));
        List<SchemaApiTestTable.SchemaApiTestRow> rows = Lists
                .newArrayList(rowKeys)
                .stream()
                .map(SchemaApiTestTable.SchemaApiTestRow::of)
                .collect(Collectors.toList());

        NavigableMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), colSelection);
        return results
                .values()
                .stream()
                .map(entry -> SchemaApiTestTable.SchemaApiTestRowResult.of(entry))
                .collect(Collectors.toMap(
                     entry -> entry.getRowName().getComponent1(), 
                     SchemaApiTestTable.SchemaApiTestRowResult::getColumn2));
    }

    /**
     * Returns a mapping from all the row keys in a rangeRequest to their value at column Column2
     * (if that column exists for the row-key). As the Column2 values are all loaded in memory,
     * do not use for large amounts of data. The order of results is preserved in the map.
     */
    public LinkedHashMap<String, StringValue> getSmallRowRangeColumn2(RangeRequest rangeRequest) {
        ColumnSelection colSelection =
                ColumnSelection.create(ImmutableList.of(PtBytes.toCachedBytes("d")));
        rangeRequest = rangeRequest.getBuilder().retainColumns(colSelection).build();
        Preconditions.checkArgument(rangeRequest.getColumnNames().size() <= 1,
                "Must not request columns other than Column2.");

        LinkedHashMap<String, StringValue> resultsMap = new LinkedHashMap<>();
        BatchingVisitableView.of(t.getRange(tableRef, rangeRequest))
                .immutableCopy().forEach(entry -> {
                     SchemaApiTestTable.SchemaApiTestRowResult resultEntry =
                          SchemaApiTestTable.SchemaApiTestRowResult.of(entry);
                     resultsMap.put(resultEntry.getRowName().getComponent1(), resultEntry.getColumn2());
                });
        return resultsMap;
    }

    /**
     * Returns a mapping from all the row keys in a range to their value at column Column2
     * (if that column exists for the row-key). As the Column2 values are all loaded in memory,
     * do not use for large amounts of data. The order of results is preserved in the map.
     */
    public LinkedHashMap<String, StringValue> getSmallRowRangeColumn2(String startInclusive,
            String endExclusive) {
        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(SchemaApiTestTable.SchemaApiTestRow.of(startInclusive).persistToBytes())
                .endRowExclusive(SchemaApiTestTable.SchemaApiTestRow.of(endExclusive).persistToBytes())
                .build();
        return getSmallRowRangeColumn2(rangeRequest);
    }

    /**
     * Returns a mapping from the first sizeLimit row keys in a rangeRequest to their value
     * at column Column2 (if that column exists). As the Column2 entries are all loaded in memory,
     * do not use for large values of sizeLimit. The order of results is preserved in the map.
     */
    public LinkedHashMap<String, StringValue> getSmallRowRangeColumn2(RangeRequest rangeRequest,
            int sizeLimit) {
        ColumnSelection colSelection =
                ColumnSelection.create(ImmutableList.of(PtBytes.toCachedBytes("d")));
        rangeRequest = rangeRequest.getBuilder().retainColumns(colSelection).batchHint(sizeLimit).build();
        Preconditions.checkArgument(rangeRequest.getColumnNames().size() <= 1,
                "Must not request columns other than Column2.");

        LinkedHashMap<String, StringValue> resultsMap = new LinkedHashMap<>();
        BatchingVisitableView.of(t.getRange(tableRef, rangeRequest))
                .batchAccept(sizeLimit, batch -> {
                     batch.forEach(entry -> {
                         SchemaApiTestTable.SchemaApiTestRowResult resultEntry =
                              SchemaApiTestTable.SchemaApiTestRowResult.of(entry);
                         resultsMap.put(resultEntry.getRowName().getComponent1(), resultEntry.getColumn2());
                     });
                     return false; // stops the traversal after the first batch
                });
        return resultsMap;
    }

    /**
     * Delete all columns for specified row components.
     */
    public void deleteRow(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] rowBytes = row.persistToBytes();
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(2);
        cells.add(Cell.create(rowBytes, PtBytes.toCachedBytes("c")));
        cells.add(Cell.create(rowBytes, PtBytes.toCachedBytes("d")));
        t.delete(tableRef, cells);
    }

    /**
     * Delete the value at column Column1 (if it exists) for the specified row-key.
     */
    public void deleteColumn1(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] rowBytes = row.persistToBytes();
        Set<Cell> cells = ImmutableSet.of(Cell.create(rowBytes, PtBytes.toCachedBytes("c")));
        t.delete(tableRef, cells);
    }

    /**
     * Delete the value at column Column2 (if it exists) for the specified row-key.
     */
    public void deleteColumn2(String component1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        byte[] rowBytes = row.persistToBytes();
        Set<Cell> cells = ImmutableSet.of(Cell.create(rowBytes, PtBytes.toCachedBytes("d")));
        t.delete(tableRef, cells);
    }

    /**
     * Takes the row-keys and a value to be inserted at column Column1.
     */
    public void putColumn1(String component1, long column1) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        t.put(tableRef, ColumnValues.toCellValues(ImmutableMultimap.of(row, SchemaApiTestTable.Column1.of(column1))));
    }

    /**
     * Takes a function that would update the value at column Column1, for the specified row
     * components. No effect if there is no value at that column. Doesn't do an additional
     * write if the new value is the same as the old one.
     */
    public void updateColumn1(String component1, Function<Long, Long> processor) {
        Optional<Long> result = getColumn1(component1);
        if (result.isPresent()) {
            Long newValue = processor.apply(result.get());
            if (Objects.equals(newValue, result.get()) == false) {
                putColumn1(component1, processor.apply(result.get()));
            }
        }
    }

    /**
     * Takes the row-keys and a value to be inserted at column Column2.
     */
    public void putColumn2(String component1, StringValue column2) {
        SchemaApiTestTable.SchemaApiTestRow row = SchemaApiTestTable.SchemaApiTestRow.of(component1);
        t.put(tableRef, ColumnValues.toCellValues(ImmutableMultimap.of(row, SchemaApiTestTable.Column2.of(column2))));
    }

    /**
     * Takes a function that would update the value at column Column2, for the specified row
     * components. No effect if there is no value at that column. Doesn't do an additional
     * write if the new value is the same as the old one.
     */
    public void updateColumn2(String component1, Function<StringValue, StringValue> processor) {
        Optional<StringValue> result = getColumn2(component1);
        if (result.isPresent()) {
            StringValue newValue = processor.apply(result.get());
            if (Objects.equals(newValue, result.get()) == false) {
                putColumn2(component1, processor.apply(result.get()));
            }
        }
    }
}
