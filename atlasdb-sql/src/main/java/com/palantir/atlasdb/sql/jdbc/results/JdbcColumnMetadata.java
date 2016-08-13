package com.palantir.atlasdb.sql.jdbc.results;

import java.util.Collection;

import com.google.protobuf.Message;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.ValueType;

public interface JdbcColumnMetadata {
    ColumnValueDescription.Format getFormat();

    ValueType getValueType();

    String getLabel();

    String getName();

    Message hydrateProto(byte[] val);

    @Override
    String toString();

    boolean isDynCol();
    boolean isNamedCol();
    boolean isRowComp();
    boolean isColComp();

    static boolean anyDynamicColumns(Collection<JdbcColumnMetadata> allCols) {
        return allCols.stream().anyMatch(JdbcColumnMetadata::isDynCol);
    }
}
