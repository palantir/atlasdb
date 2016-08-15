package com.palantir.atlasdb.sql.jdbc.results.parsed;

import java.util.Arrays;
import java.util.EnumSet;

import com.palantir.atlasdb.proto.fork.ForkedJsonFormat;
import com.palantir.atlasdb.sql.jdbc.results.JdbcReturnType;
import com.palantir.atlasdb.sql.jdbc.results.columns.QueryColumn;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ValueType;

public abstract class ResultDeserializers {

    private ResultDeserializers() {
        // uninstantiable
    }

    public static Object convert(QueryColumn res, JdbcReturnType returnType) {
        switch (returnType) {
            case BYTES:
                return res.getRawValue();
            case OBJECT:
                switch (res.getFormat()) { // inspired by AtlasSerializers.serialize
                    case PROTO:
                        return res.getValueAsMessage();
                    case PERSISTABLE:
                    case PERSISTER:
                    case VALUE_TYPE:
                        return res.getValueAsSimpleType();
                }
                break;
            case STRING:
                switch (res.getFormat()) {
                    case PROTO:
                        return ForkedJsonFormat.printToString(res.getValueAsMessage());
                    case PERSISTABLE:
                    case PERSISTER:
                    case VALUE_TYPE:
                        Object ret = res.getValueAsSimpleType();
                        return (ret instanceof  byte[])
                                ? Arrays.toString((byte[]) ret)
                                : ret;
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
                if (!EnumSet.of(ValueType.STRING, ValueType.VAR_STRING, ValueType.BLOB,
                        ValueType.SHA256HASH, ValueType.SIZED_BLOB, ValueType.UUID).contains(res.getValueType())) {
                    return res.getValueAsSimpleType();
                }
                break;
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
            throw new UnsupportedOperationException(
                    String.format("Parsing format %s (%s) as type %s is unsupported.",
                            res.getFormat(),
                            res.getValueType(),
                            returnType));
        } else {
            throw new UnsupportedOperationException(
                    String.format("Parsing format %s as type %s is unsupported.", res.getFormat(), returnType));
        }
    }

}
