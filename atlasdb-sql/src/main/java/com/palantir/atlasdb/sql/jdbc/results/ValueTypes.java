package com.palantir.atlasdb.sql.jdbc.results;

import java.sql.Types;

import com.palantir.atlasdb.table.description.ValueType;

public class ValueTypes {
    public static boolean isSigned(ValueType type) {
        switch (type) {
            case VAR_LONG:
                return false;
            case VAR_SIGNED_LONG:
                return true;
            case FIXED_LONG:
                return true;
            case FIXED_LONG_LITTLE_ENDIAN:
                return true;
            case SHA256HASH:
                return false;
            case VAR_STRING:
                return false;
            case STRING:
                return false;
            case BLOB:
                return false;
            case SIZED_BLOB:
                return false;
            case NULLABLE_FIXED_LONG:
                return false;
            case UUID:
                return false;
        }
        throw new UnsupportedOperationException("Unknown value type: " + type);
    }

    public static int getColumnType(ValueType type) {
        switch (type) {
            case VAR_LONG:
                return Types.LONGVARCHAR;
            case VAR_SIGNED_LONG:
                return Types.LONGNVARCHAR;
            case FIXED_LONG:
                return Types.BIGINT;
            case FIXED_LONG_LITTLE_ENDIAN:
                return Types.BIGINT;
            case SHA256HASH:
                return Types.BLOB;
            case VAR_STRING:
                return Types.BLOB;
            case STRING:
                return Types.BLOB;
            case BLOB:
                return Types.BLOB;
            case SIZED_BLOB:
                return Types.BLOB;
            case NULLABLE_FIXED_LONG:
                return Types.BIGINT;
            case UUID:
                return Types.BLOB;
        }
        throw new UnsupportedOperationException("Unknown value type: " + type);
    }

}
