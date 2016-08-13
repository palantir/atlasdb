package com.palantir.atlasdb.sql.jdbc.results;

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
import com.palantir.atlasdb.table.description.ValueType;

/**  A row of results.
 *
 */
public class ParsedRowResult {

    private final List<JdbcColumnMetadataAndValue> result;
    private final Map<String, JdbcColumnMetadataAndValue> index;

    static Iterator<ParsedRowResult> makeIterator(Iterable<RowResult<byte[]>> results,
                                                  Predicate<ParsedRowResult> predicate,
                                                  List<SelectableJdbcColumnMetadata> columns) {
        return StreamSupport.stream(StreamSupport.stream(results.spliterator(), false)
                                                 .flatMap(it -> create(it, columns).stream())
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
                                                List<SelectableJdbcColumnMetadata> colsMeta) {
        List<JdbcColumnMetadata> selectColsMeta = colsMeta.stream()
                .map(SelectableJdbcColumnMetadata::getMetadata)
                .collect(Collectors.toList());
        List<JdbcColumnMetadataAndValue> rowComps = parseComponents(
                rawResult.getRowName(),
                colsMeta.stream()
                        .filter(c -> c.getMetadata().isRowComp())
                        .collect(Collectors.toList()));
        if (JdbcColumnMetadata.anyDynamicColumns(selectColsMeta)) {
            List<List<JdbcColumnMetadataAndValue>> dynCols = parseColumnComponents(rawResult.getColumns(), colsMeta);
            return dynCols.stream()
                    .map(cols -> createParsedRowResult(concat(rowComps, cols)))
                    .collect(Collectors.toList());
        } else {
            List<JdbcColumnMetadataAndValue> namedCols = parseNamedColumns(rawResult.getColumns(),
                    colsMeta.stream()
                            .filter(c -> c.getMetadata().isNamedCol())
                            .collect(Collectors.toList()));
            return Collections.singletonList(createParsedRowResult(concat(rowComps, namedCols)));
        }
    }

    private static List<JdbcColumnMetadataAndValue> parseComponents(byte[] row, List<SelectableJdbcColumnMetadata> colsMeta) {
        ImmutableList.Builder<JdbcColumnMetadataAndValue> ret = ImmutableList.builder();
        int index = 0;
        for (int i = 0; i < colsMeta.size(); i++) {
            SelectableJdbcColumnMetadata meta = colsMeta.get(i);
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
                ret.add(new JdbcColumnMetadataAndValue(meta.getMetadata(), rowBytes));
            }
        }
        return ret.build();
    }

    private static List<JdbcColumnMetadataAndValue> parseNamedColumns(SortedMap<byte[], byte[]> columns,
                                                                      List<SelectableJdbcColumnMetadata> colsMeta) {
        ImmutableList.Builder<JdbcColumnMetadataAndValue> ret = ImmutableList.builder();
        Map<ByteBuffer, byte[]> wrappedCols = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
            wrappedCols.put(ByteBuffer.wrap(entry.getKey()), entry.getValue());
        }
        for (SelectableJdbcColumnMetadata column : colsMeta) {
            Preconditions.checkState(column.getMetadata().isNamedCol(), "metadata must be for named columns");
            if (!column.isSelected()) {
                continue;
            }

            ByteBuffer shortName = ByteBuffer.wrap(column.getMetadata().getName().getBytes());
            if (wrappedCols.containsKey(shortName)) {
                ret.add(JdbcColumnMetadataAndValue.create(column.getMetadata(), wrappedCols.get(shortName)));
            } else {
                ret.add(JdbcColumnMetadataAndValue.create(column.getMetadata(), new byte[0]));  // empty byte[] for missing columns
            }
        }
        return ret.build();
    }

    private static List<List<JdbcColumnMetadataAndValue>> parseColumnComponents(SortedMap<byte[], byte[]> columns,
                                                                                List<SelectableJdbcColumnMetadata> colsMeta) {
        List<SelectableJdbcColumnMetadata> dynColsMeta = colsMeta.stream()
                .filter(c -> c.getMetadata().isColComp())
                .collect(Collectors.toList());
        return columns.entrySet().stream()
                .map(e -> {
                    List<JdbcColumnMetadataAndValue> components = parseComponents(e.getKey(), dynColsMeta);
                    SelectableJdbcColumnMetadata valueMeta = Iterables.getOnlyElement(colsMeta.stream()
                            .filter(c -> c.getMetadata().isValueCol())
                            .collect(Collectors.toList()));
                    if (valueMeta.isSelected()) {
                        return ImmutableList.<JdbcColumnMetadataAndValue>builder()
                                .addAll(components)
                                .add(new JdbcColumnMetadataAndValue(valueMeta.getMetadata(), e.getValue()))
                                .build();
                    }
                    return components;
                })
                .collect(Collectors.toList());
    }

    private static ParsedRowResult createParsedRowResult(List<JdbcColumnMetadataAndValue> cols) {
        return new ParsedRowResult(cols, buildIndex(cols));
    }

    private static ImmutableMap<String, JdbcColumnMetadataAndValue> buildIndex(List<JdbcColumnMetadataAndValue> colsMeta) {
        ImmutableMap.Builder<String, JdbcColumnMetadataAndValue> indexBuilder = ImmutableMap.builder();
        indexBuilder.putAll(colsMeta.stream().collect(Collectors.toMap(JdbcColumnMetadataAndValue::getName, Function.identity())));
        indexBuilder.putAll(colsMeta.stream()
                                    .filter(m -> !m.getLabel().equals(m.getName()))
                                    .collect(Collectors.toMap(JdbcColumnMetadataAndValue::getLabel, Function.identity())));
        return indexBuilder.build();
    }

    private static List<JdbcColumnMetadataAndValue> concat(List<JdbcColumnMetadataAndValue> list1, List<JdbcColumnMetadataAndValue> list2) {
        return ImmutableList.<JdbcColumnMetadataAndValue>builder()
                                    .addAll(list1)
                                    .addAll(list2)
                                    .build();
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
