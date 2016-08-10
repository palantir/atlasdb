package com.palantir.atlasdb.sql.jdbc;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;

public class ParsedRowResult {

    private final List<MetadataAndValue> result;
    private final List<String> colLabels;

    public static ParsedRowResult create(RowResult<byte[]> rawResult, TableMetadata metadata) {
        if (metadata.getColumns().getDynamicColumn() != null) {
            throw new UnsupportedOperationException("dynamic columns are not currently supported");
        }

        ImmutableList.Builder<MetadataAndValue> resultBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> indexBuilder = ImmutableList.builder();
        parseRowComponents(rawResult.getRowName(), metadata.getRowMetadata().getRowParts(), resultBuilder, indexBuilder);
        parseColumns(rawResult, metadata.getColumns(), resultBuilder, indexBuilder);
        return new ParsedRowResult(resultBuilder.build(), indexBuilder.build());
    }

    private static void parseColumns(RowResult<byte[]> rawResult,
                                     ColumnMetadataDescription columns,
                                     ImmutableList.Builder<MetadataAndValue> resultBuilder,
                                     ImmutableList.Builder<String> indexBuilder) {
        Map<ByteBuffer, byte[]> wrappedCols = Maps.newHashMap();
        for(Map.Entry<byte[], byte[]> entry : rawResult.getColumns().entrySet()) {
            wrappedCols.put(ByteBuffer.wrap(entry.getKey()), entry.getValue());
        }
        for (NamedColumnDescription col : columns.getNamedColumns()) {
            ByteBuffer bytes = ByteBuffer.wrap(col.getShortName().getBytes());
            Preconditions.checkState(wrappedCols.containsKey(bytes), String.format("column %s is missing from results", col.getLongName()));
            resultBuilder.add(MetadataAndValue.create(col, wrappedCols.get(bytes)));
            indexBuilder.add(col.getLongName());
        }
    }

    private static void parseRowComponents(byte[] row,
                                           List<NameComponentDescription> rowParts,
                                           ImmutableList.Builder<MetadataAndValue> resultBuilder,
                                           ImmutableList.Builder<String> indexBuilder) {
        int index = 0;
        for (int i = 0; i < rowParts.size(); i++) {
            NameComponentDescription rowComp = rowParts.get(i);
            Object val = rowComp.getType().convertToJava(row, index);
            int len = rowComp.getType().sizeOf(val);
            if (len == 0) {
                Preconditions.checkArgument(rowComp.getType() == ValueType.STRING || rowComp.getType() == ValueType.BLOB,
                        "only BLOB and STRING can have unknown length");
                Preconditions.checkArgument(i == rowParts.size() - 1, "only terminal types can have unknown length");
                len = row.length - index;
            }
            byte[] rowBytes = Arrays.copyOfRange(row, index, index + len);
            index += len;
            resultBuilder.add(MetadataAndValue.create(rowComp, rowBytes));
            indexBuilder.add(rowComp.getComponentName());
        }
    }

    public ParsedRowResult(List<MetadataAndValue> result, ImmutableList<String> colLabels) {
        this.result = result;
        this.colLabels = colLabels;
    }

    public Object get(int index, JdbcReturnType returnType) throws SQLException {
        if (index >= result.size()) {
            throw new SQLException(String.format("given column index %s, but there are only %s columns", index, result.size()));
        }

        MetadataAndValue r = result.get(index);

        switch (returnType) {
            case BYTES:
                return r.getValue();
            case STRING:
                switch (r.getFormat()) {
                    case PERSISTABLE:
                    case PERSISTER:
                    case PROTO:
                        // TODO implement string formatting for non-value-types
                        break;
                    case VALUE_TYPE:
                        return r.getValueType().convertToJava(r.getValue(), 0);
                }
                break;
            // TODO implement other types
            case BYTE:
                break;
            case BOOLEAN:
                break;
            case SHORT:
                break;
            case INT:
                break;
            case LONG:
                break;
            case FLOAT:
                break;
            case DOUBLE:
                break;
            case BIG_DECIMAL:
                break;
            case OBJECT:
                break;
            case TIME:
                break;
            case TIMESTAMP:
                break;
            case DATE:
                break;
            case ASCII_STREAM:
                break;
            case BINARY_STREAM:
                break;
            case CHAR_STREAM:
                break;
        }

        if (r.getFormat() == ColumnValueDescription.Format.VALUE_TYPE) {
            throw new UnsupportedOperationException(String.format("parsing format %s (%s) as type %s is unsupported",
                    r.getFormat(),
                    r.getValueType(),
                    returnType));
        } else {
            throw new UnsupportedOperationException(String.format("parsing format %s as type %s is unsupported", r.getFormat(), returnType));
        }
    }

    public Object get(String col, JdbcReturnType returnType) throws SQLException {
        return get(getIndexFromColumnLabel(col), returnType);
    }

    public int getIndexFromColumnLabel(String col) throws SQLException {
        int index = colLabels.indexOf(col);
        if (index == -1) {
            throw new SQLException(String.format("column '%s' is not found in results", col));
        }
        return index;
    }

    private static class MetadataAndValue {
        private final Optional<NameComponentDescription> rowComp;
        private final Optional<NamedColumnDescription> col;
        private final byte[] val;

        static MetadataAndValue create(NameComponentDescription col, byte[] val) {
            return new MetadataAndValue(Optional.of(col), Optional.absent(), val);
        }

        static MetadataAndValue create(NamedColumnDescription col, byte[] val) {
            return new MetadataAndValue(Optional.absent(), Optional.of(col), val);
        }

        MetadataAndValue(Optional<NameComponentDescription> rowComp, Optional<NamedColumnDescription> col, byte[] val) {
            Preconditions.checkArgument(rowComp.isPresent() ^ col.isPresent(), "only one description should be present");
            this.rowComp = rowComp;
            this.col = col;
            this.val = val;
        }

        boolean isRowComp() {
            return rowComp.isPresent();
        }

        boolean isCol() {
            return col.isPresent();
        }

        ColumnValueDescription.Format getFormat() {
            return isRowComp() ? ColumnValueDescription.Format.VALUE_TYPE : col.get().getValue().getFormat();
        }

        ValueType getValueType() {
            return isRowComp() ? rowComp.get().getType() : col.get().getValue().getValueType();
        }

        byte[] getValue() {
            return val;
        }
    }
}
