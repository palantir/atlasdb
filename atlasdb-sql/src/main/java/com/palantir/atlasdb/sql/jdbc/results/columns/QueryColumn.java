package com.palantir.atlasdb.sql.jdbc.results.columns;

import java.util.Arrays;

import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ValueType;

public class QueryColumn {
    private final QueryColumnMetadata wrapped;
    private final byte[] rawVal;

    public static QueryColumn create(QueryColumnMetadata wrapped, byte[] rawVal) {
        return new QueryColumn(wrapped, rawVal);
    }

    private QueryColumn(QueryColumnMetadata wrapped, byte[] rawVal) {
        this.wrapped = wrapped;
        this.rawVal = rawVal;
    }

    public ColumnValueDescription.Format getFormat() {
        return wrapped.getMetadata().getFormat();
    }

    public ValueType getValueType() {
        return wrapped.getMetadata().getValueType();
    }

    public String getName() {
        return wrapped.getMetadata().getName();
    }

    public String getLabel() {
        return wrapped.getMetadata().getLabel();
    }

    public Message getValueAsMessage() {
        return wrapped.getMetadata().hydrateProto(rawVal);
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
                .add("meta", wrapped.getMetadata())
                .add("rawVal", Arrays.toString(rawVal))
                .toString();
    }
}
