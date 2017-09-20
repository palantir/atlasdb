package com.palantir.atlasdb.cas.generated;

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
public final class CheckAndSetV2Table {
    private static final String rawTableName = "check_and_set";

    private final Transaction t;

    private final TableReference tableRef;

    private CheckAndSetV2Table(Transaction t, Namespace namespace) {
        this.tableRef = TableReference.create(namespace, rawTableName);
        this.t = t;
    }

    public static CheckAndSetV2Table of(Transaction t, Namespace namespace) {
        return new CheckAndSetV2Table(t, namespace);
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

    public Optional<Long> getValue(Long id) {
        CheckAndSetTable.CheckAndSetRow row = CheckAndSetTable.CheckAndSetRow.of(id);
        byte[] bytes = row.persistToBytes();
        ColumnSelection colSelection = 
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("v")));
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), colSelection).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        }
        else {
            return Optional.of(CheckAndSetTable.CheckAndSetRowResult.of(rowResult).getValue());
        }
    }

    public Map<Long, Long> getValue(Iterable<Long> rowKeys) {
        ColumnSelection colSelection = 
                 ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("v")));
        List<CheckAndSetTable.CheckAndSetRow> rows = Lists
                .newArrayList(rowKeys)
                .stream()
                .map(CheckAndSetTable.CheckAndSetRow::of)
                .collect(Collectors.toList());
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), colSelection);
        return results
                .values()
                .stream()
                .map(entry -> CheckAndSetTable.CheckAndSetRowResult.of(entry))
                .collect(Collectors.toMap(
                     entry -> entry.getRowName().getId(), 
                     CheckAndSetTable.CheckAndSetRowResult::getValue));
    }

    public Map<Long, Long> getAllValue() {
        ColumnSelection colSelection = 
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("v")));
        return getRangeValue(RangeRequest.all());
    }

    private Map<Long, Long> getRangeValue(RangeRequest rangeRequest) {
        ColumnSelection colSelection = 
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("v")));
        rangeRequest = rangeRequest.getBuilder().retainColumns(colSelection).build();
        return BatchingVisitableView.of(t.getRange(tableRef, rangeRequest))
                .immutableCopy()
                .stream()
                .map(entry -> CheckAndSetTable.CheckAndSetRowResult.of(entry))
                .collect(Collectors.toMap(
                     entry -> entry.getRowName().getId(), 
                     CheckAndSetTable.CheckAndSetRowResult::getValue));
    }

    public void deleteRow(Long id) {
        CheckAndSetTable.CheckAndSetRow row = CheckAndSetTable.CheckAndSetRow.of(id);
        byte[] rowBytes = row.persistToBytes();
        Set<Cell> cells = new HashSet<>();
        cells.add(Cell.create(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableRef, cells);
    }

    public void deleteValue(Long id) {
        CheckAndSetTable.CheckAndSetRow row = CheckAndSetTable.CheckAndSetRow.of(id);
        byte[] rowBytes = row.persistToBytes();
        Set<Cell> cells = ImmutableSet.of(Cell.create(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableRef, cells);
    }

    public void putValue(Long id, Long value) {
        CheckAndSetTable.CheckAndSetRow row = CheckAndSetTable.CheckAndSetRow.of(id);
        t.put(tableRef, ColumnValues.toCellValues(ImmutableMultimap.of(row, CheckAndSetTable.Value.of(value))));
    }

    public void updateValue(Long id, Function<Long, Long> processor) {
        Optional<Long> result = getValue(id);
        if (result.isPresent()) {
            Long newValue = processor.apply(result.get());
            if (newValue != result.get()) {
                putValue(id, processor.apply(result.get()));
            }
        }
    }
}
