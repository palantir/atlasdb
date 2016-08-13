package com.palantir.atlasdb.sql.jdbc.results;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
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
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.annotation.Output;

/**  A row of results.
 *
 */
public class ParsedRowResult {

    private final List<JdbcColumnMetadataAndValue> result;
    private final Map<String, JdbcColumnMetadataAndValue> index;

    static Iterator<ParsedRowResult> makeIterator(Iterable<RowResult<byte[]>> results,
                                                  Predicate<ParsedRowResult> predicate,
                                                  List<JdbcColumnMetadata> columns,
                                                  Map<String, JdbcColumnMetadataAndValue> labelOrNameToResult) {
        return StreamSupport.stream(StreamSupport.stream(results.spliterator(), false)
                                                 .flatMap(it -> create(it, columns, labelOrNameToResult).stream())
                                                 .collect(Collectors.toList())
                                                 .spliterator(), false)
                            .filter(predicate)
                            .collect(Collectors.toList())
                            .iterator();
    }

    /** Create a result from a raw result. {@code columns} is a list of selected columns (or all columns, if the columns are specified),
     * or empty, if the columns are dynamic.
     */
    private static List<ParsedRowResult> create(RowResult<byte[]> rawResult,
                                                List<JdbcColumnMetadata> selectedColumns,
                                                Map<String, JdbcColumnMetadataAndValue> index) {
        ImmutableList.Builder<JdbcColumnMetadataAndValue> rowBuilder = ImmutableList.builder();
        parseRowComponents(rawResult.getRowName(),
                           selectedColumns.stream().filter(JdbcColumnMetadata::isRowComp).collect(Collectors.toList()),
                           rowBuilder);
        if (selectedColumns.size() == 0) { // dynamic columns
            // unwrap dynamic column: row + each component of rawResult.columns().
            final ImmutableList.Builder<ParsedRowResult> resultBuilder = ImmutableList.builder();
            for (Map.Entry<byte[], byte[]> entry : rawResult.getColumns().entrySet()) {
                ImmutableList.Builder<JdbcColumnMetadataAndValue> colsBuilder = ImmutableList.builder();
                colsBuilder.addAll(rowBuilder.build());
                // unpack the column *components*
                parseColumns(null, rawResult.getColumns(), colsBuilder);
                final ImmutableList<JdbcColumnMetadataAndValue> cols = rowBuilder.build();
                resultBuilder.add(new ParsedRowResult(cols, buildIndex(cols)));
            }
            return resultBuilder.build();
        } else {

            parseColumns(
                    selectedColumns.stream().filter(JdbcColumnMetadata::isCol).collect(Collectors.toList()),
                    rawResult.getColumns(), rowBuilder);
            List<JdbcColumnMetadataAndValue> colsMeta = rowBuilder.build();
            return Collections.singletonList(new ParsedRowResult(colsMeta, buildIndex(colsMeta)));
        }
    }

    private static ImmutableMap<String, JdbcColumnMetadataAndValue> buildIndex(List<JdbcColumnMetadataAndValue> colsMeta) {
        ImmutableMap.Builder<String, JdbcColumnMetadataAndValue> indexBuilder = ImmutableMap.builder();
        indexBuilder.putAll(colsMeta.stream().collect(Collectors.toMap(JdbcColumnMetadataAndValue::getName, Function.identity())));
        indexBuilder.putAll(colsMeta.stream()
                                    .filter(m -> !m.getLabel().equals(m.getName()))
                                    .collect(Collectors.toMap(JdbcColumnMetadataAndValue::getLabel, Function.identity())));
        return indexBuilder.build();
    }

    private static void parseColumns(Collection<JdbcColumnMetadata> colsMeta,
                                     SortedMap<byte[], byte[]> rawColumns,
                                     @Output ImmutableList.Builder<JdbcColumnMetadataAndValue> resultBuilder) {
        Map<ByteBuffer, byte[]> wrappedCols = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> entry : rawColumns.entrySet()) {
            wrappedCols.put(ByteBuffer.wrap(entry.getKey()), entry.getValue());
        }
        for (JdbcColumnMetadata column : colsMeta) {
            Preconditions.checkState(column.isCol(), "all metadata here is expected to be for columns");
            ByteBuffer shortName = ByteBuffer.wrap(column.getName().getBytes());
            if (wrappedCols.containsKey(shortName)) {
                resultBuilder.add(JdbcColumnMetadataAndValue.create(column, wrappedCols.get(shortName)));
            } else {
                resultBuilder.add(JdbcColumnMetadataAndValue.create(column, new byte[0]));  // empty byte[] for missing columns
            }
        }
    }

    private static void parseColumnComponents(RowResult<byte[]> rawResult,
                                              List<JdbcColumnMetadata> colsMeta,
                                              @Output ImmutableList.Builder<JdbcColumnMetadataAndValue> resultBuilder) {
        //TODO
    }

    private static void parseRowComponents(byte[] row,
                                           List<JdbcColumnMetadata> colsMeta,
                                           @Output ImmutableList.Builder<JdbcColumnMetadataAndValue> resultBuilder) {
        int index = 0;
        for (int i = 0; i < colsMeta.size(); i++) {
            JdbcColumnMetadata meta = colsMeta.get(i);
            Preconditions.checkState(meta.isRowComp(), "all metadata here is expected to be for rows components");

            ValueType type = meta.getValueType();
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
            resultBuilder.add(new JdbcColumnMetadataAndValue(meta, rowBytes));
        }
    }

    private ParsedRowResult(List<JdbcColumnMetadataAndValue> result, Map<String, JdbcColumnMetadataAndValue> labelToResult) {
        this.result = result;
        this.index = labelToResult;
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
}
