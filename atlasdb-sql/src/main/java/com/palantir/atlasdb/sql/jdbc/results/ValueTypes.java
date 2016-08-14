package com.palantir.atlasdb.sql.jdbc.results;

import java.sql.Types;

import com.palantir.atlasdb.table.description.ValueType;

public class ValueTypes {
    public static boolean isSigned(ValueType type) {
        switch (type) {
            case VAR_SIGNED_LONG:
            case FIXED_LONG:
            case FIXED_LONG_LITTLE_ENDIAN:
                return true;
            case VAR_LONG:
            case SHA256HASH:
            case VAR_STRING:
            case STRING:
            case BLOB:
            case SIZED_BLOB:
            case NULLABLE_FIXED_LONG:
            case UUID:
                return false;
        }
        throw new UnsupportedOperationException("Unknown value type: " + type);
    }

    public static int getColumnType(ValueType type) {
        switch (type) {
            case VAR_LONG:
            case VAR_SIGNED_LONG:
                return Types.LONGNVARCHAR;
            case FIXED_LONG:
            case FIXED_LONG_LITTLE_ENDIAN:
            case NULLABLE_FIXED_LONG:
                return Types.BIGINT;
            case SHA256HASH:
            case VAR_STRING:
            case STRING:
            case BLOB:
            case SIZED_BLOB:
            case UUID:
                return Types.BLOB;
        }
        throw new UnsupportedOperationException("Unknown value type: " + type);
    }

    public static int maxDisplaySize(ValueType type) {
        switch (type) {
            case VAR_LONG:
            case VAR_SIGNED_LONG:
            case FIXED_LONG:
            case FIXED_LONG_LITTLE_ENDIAN:
            case NULLABLE_FIXED_LONG:
                return 10;
            case STRING:
            case VAR_STRING:
            case SHA256HASH:
            case UUID:
                return 20;
            case BLOB:
            case SIZED_BLOB:
                return 40;

        }
        throw new UnsupportedOperationException("Unknown value type: " + type);
    }
}
