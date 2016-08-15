package com.palantir.atlasdb.sql.jdbc.results;

import java.util.Arrays;

import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;
import com.palantir.atlasdb.sql.grammar.AggregateFunction;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ValueType;

public class JdbcColumnMetadataAndValue extends SelectableJdbcColumnMetadata {
    private final byte[] rawVal;

    static JdbcColumnMetadataAndValue create(SelectableJdbcColumnMetadata meta, byte[] rawVal) {
        return new JdbcColumnMetadataAndValue(meta.getMetadata(), meta.getAggregateFunction(), rawVal);
    }

    JdbcColumnMetadataAndValue(JdbcColumnMetadata meta, AggregateFunction aggregateFunctions, byte[] rawVal) {
        super(meta, AggregateFunction.IDENTITY);
        this.rawVal = rawVal;
    }

    JdbcColumnMetadataAndValue(JdbcColumnMetadata meta, byte[] rawVal) {
        this(meta, AggregateFunction.IDENTITY, rawVal);
    }

    ColumnValueDescription.Format getFormat() {
        return metadata.getFormat();
    }

    ValueType getValueType() {
        return metadata.getValueType();
    }

    String getName() {
        return metadata.getName();
    }

    String getLabel() {
        return metadata.getLabel();
    }

    Message getValueAsMessage() {
        return metadata.hydrateProto(rawVal);
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
                .add("meta", metadata)
                .add("rawVal", Arrays.toString(rawVal))
                .toString();
    }
}
