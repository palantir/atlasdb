package com.palantir.atlasdb.sql.jdbc.results.parsed;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.sql.jdbc.results.JdbcReturnType;
import com.palantir.atlasdb.sql.jdbc.results.columns.JdbcColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.columns.QueryColumn;
import com.palantir.atlasdb.sql.jdbc.results.columns.QueryColumnMetadata;
import com.palantir.atlasdb.table.description.ValueType;

/**  A row of results.
 */
public class ParsedRowResult {

    private final List<QueryColumn> result;
    private final Map<String, QueryColumn> index;

    static Iterator<ParsedRowResult> parseRowResults(Iterable<RowResult<byte[]>> results,
                                                     Predicate<ParsedRowResult> predicate,
                                                     List<QueryColumnMetadata> selectedCols) {
        return StreamSupport.stream(StreamSupport.stream(results.spliterator(), false)
                .flatMap(it -> create(it, selectedCols).stream())
                .collect(Collectors.toList())
                .spliterator(), false)
                .filter(predicate)
                .iterator();
    }

    /** Create a result from a raw result. {@code columns} is a list of selected columns (or all columns, if the columns are specified),
     * or empty, if the columns are dynamic.
     */
    private static List<ParsedRowResult> create(RowResult<byte[]> rawResult,
                                                List<QueryColumnMetadata> colsMeta) {
        List<QueryColumn> rowComps = parseComponents(
                rawResult.getRowName(),
                colsMeta.stream()
                        .filter(c -> c.getMetadata().isRowComp())
                        .collect(Collectors.toList()));
        if (anyDynamicColumns(colsMeta)) {
            List<List<QueryColumn>> dynCols = parseColumnComponents(rawResult.getColumns(), colsMeta);
            return dynCols.stream()
                    .map(cols -> createParsedRowResult(concat(rowComps, cols)))
                    .collect(Collectors.toList());
        } else {
            List<QueryColumn> namedCols = parseNamedColumns(rawResult.getColumns(),
                    colsMeta.stream()
                            .filter(c -> c.getMetadata().isNamedCol())
                            .collect(Collectors.toList()));
            return Collections.singletonList(createParsedRowResult(concat(rowComps, namedCols)));
        }
    }

    private static boolean anyDynamicColumns(List<QueryColumnMetadata> colsMeta) {
        return colsMeta.stream().map(QueryColumnMetadata::getMetadata).anyMatch(JdbcColumnMetadata::isColComp);
    }

    private static List<QueryColumn> parseComponents(byte[] row, List<QueryColumnMetadata> colsMeta) {
        ImmutableList.Builder<QueryColumn> ret = ImmutableList.builder();
        int index = 0;
        for (int i = 0; i < colsMeta.size(); i++) {
            QueryColumnMetadata meta = colsMeta.get(i);
            Preconditions.checkState(meta.getMetadata().isRowComp() || meta.getMetadata().isColComp(),
                    "metadata must be for components");
            ValueType type = meta.getMetadata().getValueType();
            Object val = type.convertToJava(row, index);
            int len = type.sizeOf(val);
            if (len == 0) {
                Preconditions.checkArgument(type == ValueType.STRING || type == ValueType.BLOB,
                        "only BLOB and STRING can have unknown length");
                Preconditions.checkArgument(i == colsMeta.size() - 1, "only terminal types can have unknown length");
                len = row.length - index;
            }
            byte[] rowBytes = Arrays.copyOfRange(row, index, index + len);
            index += len;
            if (meta.isSelected()) {
                ret.add(QueryColumn.create(meta, rowBytes));
            }
        }
        return ret.build();
    }

    private static List<QueryColumn> parseNamedColumns(SortedMap<byte[], byte[]> columns,
                                                       List<QueryColumnMetadata> colsMeta) {
        ImmutableList.Builder<QueryColumn> ret = ImmutableList.builder();
        Map<ByteBuffer, byte[]> wrappedCols = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
            wrappedCols.put(ByteBuffer.wrap(entry.getKey()), entry.getValue());
        }
        for (QueryColumnMetadata column : colsMeta) {
            Preconditions.checkState(column.getMetadata().isNamedCol(), "metadata must be for named columns");
            if (!column.isSelected()) {
                continue;
            }

            ByteBuffer shortName = ByteBuffer.wrap(column.getMetadata().getName().getBytes());
            if (wrappedCols.containsKey(shortName)) {
                ret.add(QueryColumn.create(column, wrappedCols.get(shortName)));
            } else {
                ret.add(QueryColumn.create(column, new byte[0]));  // empty byte[] for missing columns
            }
        }
        return ret.build();
    }

    private static List<List<QueryColumn>> parseColumnComponents(SortedMap<byte[], byte[]> columns,
                                                                 List<QueryColumnMetadata> colsMeta) {
        List<QueryColumnMetadata> dynColsMeta = colsMeta.stream()
                .filter(c -> c.getMetadata().isColComp())
                .collect(Collectors.toList());
        return columns.entrySet().stream()
                .map(e -> {
                    List<QueryColumn> components = parseComponents(e.getKey(), dynColsMeta);
                    QueryColumnMetadata valueMeta = Iterables.getOnlyElement(colsMeta.stream()
                            .filter(c -> c.getMetadata().isValueCol())
                            .collect(Collectors.toList()));
                    if (valueMeta.isSelected()) {
                        return ImmutableList.<QueryColumn>builder()
                                .addAll(components)
                                .add(QueryColumn.create(valueMeta, e.getValue()))
                                .build();
                    }
                    return components;
                })
                .collect(Collectors.toList());
    }

    private static ParsedRowResult createParsedRowResult(List<QueryColumn> cols) {
        return new ParsedRowResult(cols, buildIndex(cols));
    }

    private static ImmutableMap<String, QueryColumn> buildIndex(List<QueryColumn> colsMeta) {
        ImmutableMap.Builder<String, QueryColumn> indexBuilder = ImmutableMap.builder();
        indexBuilder.putAll(colsMeta.stream().collect(Collectors.toMap(QueryColumn::getName, Function.identity())));
        indexBuilder.putAll(colsMeta.stream()
                .filter(m -> !m.getLabel().equals(m.getName()))
                .collect(Collectors.toMap(QueryColumn::getLabel, Function.identity())));
        return indexBuilder.build();
    }

    private static List<QueryColumn> concat(List<QueryColumn> list1, List<QueryColumn> list2) {
        return ImmutableList.<QueryColumn>builder()
                .addAll(list1)
                .addAll(list2)
                .build();
    }

    public static ParsedRowResult create(List<QueryColumn> result) {
        return new ParsedRowResult(result, buildIndex(result));
    }

    private ParsedRowResult(List<QueryColumn> result, Map<String, QueryColumn> index) {
        this.result = result;
        this.index = index;
    }

    private Object get(QueryColumn res, JdbcReturnType returnType) {
        return ResultDeserializers.convert(res, returnType);
    }

    public Object get(int index, JdbcReturnType returnType) throws SQLException {
        if (index > result.size()) {
            throw new SQLException(String.format("given column index %s, but there are only %s columns", index, result.size()));
        }
        return get(result.get(index - 1), returnType);
    }

    public Object get(String col, JdbcReturnType returnType) throws SQLException {
        if (!index.containsKey(col)) {
            throw new SQLException(String.format("column '%s' is not found in results", col));
        }
        return get(index.get(col), returnType);
    }

    public int getIndexFromColumnLabel(String col) throws SQLException {
        if (!index.containsKey(col)) {
            throw new SQLException(String.format("column '%s' is not found in results", col));
        }
        return result.indexOf(index.get(col)) + 1;
    }

    public List<QueryColumn> getResults() {
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("result", result)
                .add("index", index)
                .toString();
    }

    /* to throw from within streams */
    public static class AggregatorException extends RuntimeException {
        public AggregatorException(Throwable cause) {
            super(cause);
        }

        public AggregatorException(String message) {
            super(message);
        }
    }

}
