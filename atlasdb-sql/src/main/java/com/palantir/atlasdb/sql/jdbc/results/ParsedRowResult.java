package com.palantir.atlasdb.sql.jdbc.results;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.proto.fork.ForkedJsonFormat;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.annotation.Output;

public class ParsedRowResult {

    private final List<MetadataAndValue> result;
    private final Map<String, MetadataAndValue> labelOrNameToResult;

    public static ParsedRowResult create(RowResult<byte[]> rawResult, List<JdbcColumnMetadata> columns) {
        if (columns.stream().anyMatch(c -> c.isDynCol())) {
            throw new UnsupportedOperationException("dynamic columns are not currently supported");
        }

        ImmutableList.Builder<MetadataAndValue> resultBuilder = ImmutableList.builder();
        parseRowComponents(rawResult.getRowName(),
                columns.stream().filter(JdbcColumnMetadata::isRowComp).collect(Collectors.toList()),
                resultBuilder);
        parseColumns(rawResult,
                columns.stream().filter(JdbcColumnMetadata::isCol).collect(Collectors.toList()),
                resultBuilder);
        List<MetadataAndValue> colsMeta = resultBuilder.build();
        ImmutableMap.Builder<String, MetadataAndValue> indexBuilder = ImmutableMap.builder();
        indexBuilder.putAll(colsMeta.stream().collect(Collectors.toMap(MetadataAndValue::getName, Function.identity())));
        indexBuilder.putAll(colsMeta.stream()
                .filter(m -> !m.getLabel().equals(m.getName()))
                .collect(Collectors.toMap(MetadataAndValue::getLabel, Function.identity())));
        return new ParsedRowResult(colsMeta, indexBuilder.build());
    }

    private static void parseColumns(RowResult<byte[]> rawResult,
                                     List<JdbcColumnMetadata> colsMeta,
                                     @Output ImmutableList.Builder<MetadataAndValue> resultBuilder) {
        Map<ByteBuffer, byte[]> wrappedCols = Maps.newHashMap();
        for(Map.Entry<byte[], byte[]> entry : rawResult.getColumns().entrySet()) {
            wrappedCols.put(ByteBuffer.wrap(entry.getKey()), entry.getValue());
        }
        for (JdbcColumnMetadata meta : colsMeta) {
            Preconditions.checkState(meta.isCol(), "all metadata here is expected to be for columns");
            ByteBuffer shortName = ByteBuffer.wrap(meta.getName().getBytes());
            if (wrappedCols.containsKey(shortName)) {
                resultBuilder.add(MetadataAndValue.create(meta, wrappedCols.get(shortName)));
            } else {
                resultBuilder.add(MetadataAndValue.create(meta, null));  // put null for missing columns
            }
        }
    }

    private static void parseRowComponents(byte[] row,
                                           List<JdbcColumnMetadata> colsMeta,
                                           @Output ImmutableList.Builder<MetadataAndValue> resultBuilder) {
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
            resultBuilder.add(new MetadataAndValue(meta, rowBytes));
        }
    }

    public ParsedRowResult(List<MetadataAndValue> result, Map<String, MetadataAndValue> labelToResult) {
        this.result = result;
        this.labelOrNameToResult = labelToResult;
    }

    private Object get(MetadataAndValue res, JdbcReturnType returnType) {
        switch (returnType) {
            case BYTES:
                return res.getRawValue();
            case OBJECT:
                switch (res.getFormat()) { // inspired by AtlasSerializers.serialize
                    case PROTO:
                        return res.getValueAsMessage();
                    case PERSISTABLE:
                        break;
                    case VALUE_TYPE:
                        return res.getValueAsSimpleType();
                    case PERSISTER:
                        return res.getValueAsSimpleType();
                }
                break;
            case STRING:
                switch (res.getFormat()) {
                    case PERSISTABLE:
                    case PERSISTER:
                        break;
                    case PROTO:
                        if (res.meta.isCol()) {
                            Message proto = res.getValueAsMessage();
                            return ForkedJsonFormat.printToString(proto);
                        } else {
                            throw new UnsupportedOperationException("Cannot (yet) parse a PROTO row component as a string.");
                        }
                    case VALUE_TYPE:
                        return res.getValueAsSimpleType();
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
                if (!EnumSet.of(ValueType.STRING, ValueType.VAR_STRING, ValueType.BLOB, ValueType.SHA256HASH, ValueType.SIZED_BLOB, ValueType.UUID)
                        .contains(res.getValueType())) {
                    return res.getValueAsSimpleType();
                }
            case FLOAT:
                break;
            case DOUBLE:
                break;
            case BIG_DECIMAL:
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

        if (res.getFormat() == ColumnValueDescription.Format.VALUE_TYPE) {
            throw new UnsupportedOperationException(String.format("parsing format %s (%s) as type %s is unsupported",
                    res.getFormat(),
                    res.getValueType(),
                    returnType));
        } else {
            throw new UnsupportedOperationException(String.format("parsing format %s as type %s is unsupported", res.getFormat(), returnType));
        }
    }

    public Object get(int index, JdbcReturnType returnType) throws SQLException {
        if (index > result.size()) {
            throw new SQLException(String.format("given column index %s, but there are only %s columns", index, result.size()));
        }
        return get(result.get(index - 1), returnType);
    }

    public Object get(String col, JdbcReturnType returnType) throws SQLException {
        if (!labelOrNameToResult.containsKey(col)) {
            throw new SQLException(String.format("column '%s' is not found in results", col));
        }
        return get(labelOrNameToResult.get(col), returnType);
    }

    public int getIndexFromColumnLabel(String col) throws SQLException {
        if (!labelOrNameToResult.containsKey(col)) {
            throw new SQLException(String.format("column '%s' is not found in results", col));
        }
        return result.indexOf(labelOrNameToResult.get(col)) + 1;
    }

    private static class MetadataAndValue {
        private final JdbcColumnMetadata meta;
        private final byte[] rawVal;

        static MetadataAndValue create(JdbcColumnMetadata meta, byte[] rawVal) {
            return new MetadataAndValue(meta, rawVal);
        }

        MetadataAndValue(JdbcColumnMetadata meta, byte[] rawVal) {
            this.meta = meta;
            this.rawVal = rawVal;
        }

        ColumnValueDescription.Format getFormat() {
            return meta.getFormat();
        }

        ValueType getValueType() {
            return meta.getValueType();
        }

        public Message getValueAsMessage() {
            return meta.hydrateProto(rawVal);
        }

        String getName() {
            return meta.getName();
        }

        String getLabel() {
            return meta.getLabel();
        }

        public Object getValueAsSimpleType() {
            return getValueType().convertToJava(getRawValue(), 0);
        }

        public byte[] getRawValue() {
            return rawVal;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("meta", meta)
                    .add("rawVal", rawVal)
                    .toString();
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("result", result)
                .add("labelOrNameToResult", labelOrNameToResult)
                .toString();
    }
}
