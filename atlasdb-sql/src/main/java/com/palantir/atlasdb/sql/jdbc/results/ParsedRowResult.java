package com.palantir.atlasdb.sql.jdbc.results;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.ValueType;

/**  A row of results.
 */
public class ParsedRowResult {

    private final List<JdbcColumnMetadataAndValue> result;
    private final Map<String, JdbcColumnMetadataAndValue> index;

    static Stream<ParsedRowResult> parseRowResults(Iterable<RowResult<byte[]>> results,
                                                   Predicate<ParsedRowResult> predicate,
                                                   List<SelectableJdbcColumnMetadata> selectedCols,
                                                   Map<String, JdbcColumnMetadata> allCols) {
        final Aggregator aggregator = new Aggregator(selectedCols);
        Stream<ParsedRowResult> stream = StreamSupport.stream(StreamSupport.stream(results.spliterator(), false)
                                                                           .flatMap(it -> create(it, selectedCols, allCols).stream())
                                                                           .collect(Collectors.toList())
                                                                           .spliterator(), false)
                                                      .filter(predicate);

        if (aggregator.toAggregate()) {
            return Stream.of(stream.reduce(aggregator.empty(), // need selected cols to know the identity values
                                           aggregator));
        } else {
            return stream;
        }
    }

    /** Create a result from a raw result. {@code columns} is a list of selected columns (or all columns, if the columns are specified),
     * or empty, if the columns are dynamic.
     */
    private static List<ParsedRowResult> create(RowResult<byte[]> rawResult,
                                                List<SelectableJdbcColumnMetadata> selectedCols,
                                                Map<String, JdbcColumnMetadata> allCols) {
        Map<JdbcColumnMetadata, byte[]> rowComps
                = parseComponents(rawResult.getRowName(),
                                  allCols.values().stream()
                                         .filter(JdbcColumnMetadata::isRowComp)
                                         .collect(Collectors.toList())).build();
        if (selectedCols.stream()
                        .map(SelectableJdbcColumnMetadata::getMetadata)
                        .anyMatch(JdbcColumnMetadata::isColComp)) {

            List<Map<JdbcColumnMetadata, byte[]>> dynCols
                    = parseColumnComponents(rawResult.getColumns(),
                                            selectedCols,
                                            allCols.values().stream()
                                                   .filter(JdbcColumnMetadata::isColComp)
                                                   .collect(Collectors.toList()));
            return dynCols.stream()
                    .map(cols -> createParsedRowResult(selectedCols, rowComps, cols))
                    .collect(Collectors.toList());
        } else {
            Map<JdbcColumnMetadata, byte[]> namedCols
                    = parseNamedColumns(rawResult.getColumns(),
                                        selectedCols);
            return Collections.singletonList(createParsedRowResult(selectedCols, rowComps, namedCols));
        }
    }

    private static ImmutableMap.Builder<JdbcColumnMetadata, byte[]> parseComponents(byte[] row,
                                                                                    List<JdbcColumnMetadata> componentsMeta) {
        ImmutableMap.Builder<JdbcColumnMetadata, byte[]> ret = ImmutableMap.builder();
        int index = 0;
        for (int i = 0; i < componentsMeta.size(); i++) {
            JdbcColumnMetadata meta = componentsMeta.get(i);
            Preconditions.checkState(meta.isRowComp() || meta.isColComp(),
                    "metadata must be for components");
            ValueType type = meta.getValueType();
            Object val = type.convertToJava(row, index);
            int len = type.sizeOf(val);
            if (len == 0) {
                Preconditions.checkArgument(type == ValueType.STRING || type == ValueType.BLOB,
                        "only BLOB and STRING can have unknown length");
                Preconditions.checkArgument(i == componentsMeta.size() - 1, "only terminal types can have unknown length");
                len = row.length - index;
            }
            byte[] rowBytes = Arrays.copyOfRange(row, index, index + len);
            index += len;
            ret.put(meta, rowBytes);
        }
        return ret;
    }

    /** Do not parse what's not selected, and do not parse selected columns twice.
     */
    private static ImmutableMap<JdbcColumnMetadata, byte[]> parseNamedColumns(SortedMap<byte[], byte[]> columns,
                                                                              List<SelectableJdbcColumnMetadata> selectedCols) {
        ImmutableMap.Builder<JdbcColumnMetadata, byte[]> ret = ImmutableMap.builder();
        Map<ByteBuffer, byte[]> wrappedCols = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
            wrappedCols.put(ByteBuffer.wrap(entry.getKey()), entry.getValue());
        }

        final Set<JdbcColumnMetadata> columnsToParse = selectedCols.stream()
                                                                   .filter(SelectableJdbcColumnMetadata::isSelected)
                                                                   .filter(it -> it.getMetadata().isNamedCol())
                                                                   .map(SelectableJdbcColumnMetadata::getMetadata)
                                                                   .collect(Collectors.toSet());

        for (JdbcColumnMetadata column : columnsToParse) {
            ByteBuffer shortName = ByteBuffer.wrap(column.getName().getBytes());
            if (wrappedCols.containsKey(shortName)) {
                ret.put(column, wrappedCols.get(shortName));
            } else {
                ret.put(column, new byte[0]);  // empty byte[] for missing columns
            }
        }
        return ret.build();
    }

    private static List<Map<JdbcColumnMetadata, byte[]>> parseColumnComponents(SortedMap<byte[], byte[]> columns,
                                                                               List<SelectableJdbcColumnMetadata> selectedCols,
                                                                               List<JdbcColumnMetadata> dynColsMeta) {
        return columns.entrySet().stream()
                .map(e -> {
                    // parse every row
                    ImmutableMap.Builder<JdbcColumnMetadata, byte[]> components = parseComponents(e.getKey(), dynColsMeta);
                    final Set<SelectableJdbcColumnMetadata> valueColumn = selectedCols.stream()
                                                                                      .filter(c -> c.getMetadata().isValueCol())
                                                                                      .collect(Collectors.toSet());
                    // if value column is selected, parse it
                    for (SelectableJdbcColumnMetadata valueMeta : valueColumn) {
                        components.put(valueMeta.getMetadata(), e.getValue());
                    }
                    return components.build();
                })
                .collect(Collectors.toList());
    }

    private static ParsedRowResult createParsedRowResult(List<SelectableJdbcColumnMetadata> selectedCols,
                                                         Map<JdbcColumnMetadata, byte[]> rows,
                                                         Map<JdbcColumnMetadata, byte[]> cols) {
        Map<JdbcColumnMetadata, byte[]> allValues = new HashMap<>(rows);
        allValues.putAll(cols);
        final List<JdbcColumnMetadataAndValue> result
                = selectedCols.stream()
                              .map(it -> new JdbcColumnMetadataAndValue(it.getMetadata(),
                                                                        it.getAggregateFunction(),
                                                                        allValues.get(it.getMetadata())))
                              .collect(Collectors.toList());
        return new ParsedRowResult(result);
    }

    private ImmutableMap<String, JdbcColumnMetadataAndValue> buildIndex(List<JdbcColumnMetadataAndValue> colsMeta) {
        ImmutableMap.Builder<String, JdbcColumnMetadataAndValue> indexBuilder = ImmutableMap.builder();
        indexBuilder.putAll(colsMeta.stream()
                                    .collect(Collectors.toMap(JdbcColumnMetadataAndValue::getName,
                                                              Function.identity())));
        indexBuilder.putAll(colsMeta.stream()
                                    .filter(m -> !m.getLabel().equals(m.getName()))
                                    .collect(Collectors.toMap(JdbcColumnMetadataAndValue::getLabel, Function.identity())));
        return indexBuilder.build();
    }

    ParsedRowResult(List<JdbcColumnMetadataAndValue> result) {
        this.result = result;
        this.index = buildIndex(result);
    }

    private Object get(JdbcColumnMetadataAndValue res, JdbcReturnType returnType) {
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

    public List<JdbcColumnMetadataAndValue> getResults() {
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
    static class AggregatorException extends RuntimeException {
        public AggregatorException(Throwable cause) {
            super(cause);
        }

        public AggregatorException(String message) {
            super(message);
        }
    }

    ParsedRowResult empty() {
        return null;
    }
}
