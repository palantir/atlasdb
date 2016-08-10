package com.palantir.atlasdb.sql.jdbc;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;

public class ParsedRowResult {

    private final List<MetadataAndData> result;
    private final Map<String, Integer> labelToIndexMap;

    public static ParsedRowResult create(RowResult<byte[]> rawResult, TableMetadata metadata) {
        if (metadata.getColumns().getDynamicColumn() != null) {
            throw new UnsupportedOperationException("dynamic columns are not currently supported");
        }

        Map<ByteBuffer, byte[]> wrappedCols = Maps.newHashMap();
        for(Map.Entry<byte[], byte[]> entry : rawResult.getColumns().entrySet()) {
            wrappedCols.put(ByteBuffer.wrap(entry.getKey()), entry.getValue());
        }

        ImmutableList.Builder<MetadataAndData> resultBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, Integer> indexBuilder = ImmutableMap.builder();
        int index = 0;
        for (NamedColumnDescription col : metadata.getColumns().getNamedColumns()) {
            ByteBuffer bytes = ByteBuffer.wrap(col.getShortName().getBytes());
            Preconditions.checkState(wrappedCols.containsKey(bytes), String.format("column %s is missing from results", col.getLongName()));
            resultBuilder.add(new MetadataAndData(col, wrappedCols.get(bytes)));
            indexBuilder.put(col.getLongName(), index++);
        }
        return new ParsedRowResult(resultBuilder.build(), indexBuilder.build());
    }

    public ParsedRowResult(List<MetadataAndData> result, Map<String, Integer> labelToIndexMap) {
        this.result = result;
        this.labelToIndexMap = labelToIndexMap;
    }

    public Object get(int index, JdbcReturnType returnType) throws SQLException {
        if (index >= result.size()) {
            throw new SQLException(String.format("given column index %s, but there are only %s columns", index, result.size()));
        }

        switch (returnType) {
            case BYTES:
                return getData(index);

            case STRING:
                switch (getFormat(index)) {
                    case PERSISTABLE:
                    case PERSISTER:
                    case PROTO:
                        // TODO implement string formatting for non-value-types
                        break;
                    case VALUE_TYPE:
                        return getValueType(index).convertToJava(getData(index), 0);
                }
                break;
        }

        if (getFormat(index) == ColumnValueDescription.Format.VALUE_TYPE) {
            throw new UnsupportedOperationException(String.format("parsing format %s (%s) as type %s is unsupported",
                    getFormat(index),
                    getValueType(index),
                    returnType));
        } else {
            throw new UnsupportedOperationException(String.format("parsing format %s as type %s is unsupported", getFormat(index), returnType));
        }
    }

    public Object get(String col, JdbcReturnType returnType) throws SQLException {
        return get(getIndexFromColumnLabel(col), returnType);
    }

    private int getIndexFromColumnLabel(String col) throws SQLException {
        if (!labelToIndexMap.containsKey(col)) {
            throw new SQLException(String.format("column '%s' is not found in results", col));
        }
        return labelToIndexMap.get(col);
    }

    private ColumnValueDescription.Format getFormat(int index) {
       return result.get(index).getColumn().getValue().getFormat();
    }

    private ValueType getValueType(int index) {
        return result.get(index).getColumn().getValue().getValueType();
    }

    private byte[] getData(int index) {
        return result.get(index).getValue();
    }

    private static class MetadataAndData {
        private final NamedColumnDescription col;
        private final byte[] val;

        public MetadataAndData(NamedColumnDescription col, byte[] val) {
            this.col = col;
            this.val = val;
        }

        public NamedColumnDescription getColumn() {
            return col;
        }

        public byte[] getValue() {
            return val;
        }
    }
}
