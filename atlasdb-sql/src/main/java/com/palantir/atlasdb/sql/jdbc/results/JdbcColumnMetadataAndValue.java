package com.palantir.atlasdb.sql.jdbc.results;

import java.util.Arrays;

import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ValueType;

public class JdbcColumnMetadataAndValue {
    private final JdbcColumnMetadata meta;
    private final byte[] rawVal;

    static JdbcColumnMetadataAndValue create(JdbcColumnMetadata meta, byte[] rawVal) {
        return new JdbcColumnMetadataAndValue(meta, rawVal);
    }

    JdbcColumnMetadataAndValue(JdbcColumnMetadata meta, byte[] rawVal) {
        this.meta = meta;
        this.rawVal = rawVal;
    }

    ColumnValueDescription.Format getFormat() {
        return meta.getFormat();
    }

    ValueType getValueType() {
        return meta.getValueType();
    }

    String getName() {
        return meta.getName();
    }

    String getLabel() {
        return meta.getLabel();
    }

    Message getValueAsMessage() {
        return meta.hydrateProto(rawVal);
    }

    Object getValueAsSimpleType() {
        return getValueType().convertToJava(getRawValue(), 0);
    }

    byte[] getRawValue() {
        return rawVal;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("meta", meta)
                .add("rawVal", Arrays.toString(rawVal))
                .toString();
    }
}
