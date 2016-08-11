package com.palantir.atlasdb.sql.jdbc.results;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.ValueType;

class JdbcColumnMetadata {
    static final String DYNAMIC_COLUMN_LABEL = "dyn";

    private final Optional<NameComponentDescription> rowComp;
    private final Optional<NamedOrDynamicColumnDescription> col;

    static JdbcColumnMetadata create(NameComponentDescription rowComp) {
        return new JdbcColumnMetadata(Optional.of(rowComp), Optional.absent());
    }

    static JdbcColumnMetadata create(NamedColumnDescription col) {
        return new JdbcColumnMetadata(Optional.absent(),
                Optional.of(new NamedOrDynamicColumnDescription(col.getLongName(), col.getShortName(), col.getValue())));
    }

    static JdbcColumnMetadata create(DynamicColumnDescription col) {
        return new JdbcColumnMetadata(Optional.absent(),
                Optional.of(new NamedOrDynamicColumnDescription(DYNAMIC_COLUMN_LABEL, DYNAMIC_COLUMN_LABEL, col.getValue())));
    }

    JdbcColumnMetadata(Optional<NameComponentDescription> rowComp, Optional<NamedOrDynamicColumnDescription> col) {
        Preconditions.checkArgument(rowComp.isPresent() ^ col.isPresent(), "only one description should be present");
        this.rowComp = rowComp;
        this.col = col;
    }

    boolean isRowComp() {
        return rowComp.isPresent();
    }

    boolean isCol() {
        return col.isPresent();
    }

    ColumnValueDescription.Format getFormat() {
        return isRowComp() ? ColumnValueDescription.Format.VALUE_TYPE : col.get().desc.getFormat();
    }

    ValueType getValueType() {
        return isRowComp() ? rowComp.get().getType() : col.get().desc.getValueType();
    }

    String getLabel() {
        return isRowComp() ? rowComp.get().getComponentName() : col.get().longName;
    }

    String getName() {
        return isRowComp() ? rowComp.get().getComponentName() : col.get().shortName;
    }

    private static class NamedOrDynamicColumnDescription {
        final String longName;
        final String shortName;
        final ColumnValueDescription desc;
        public NamedOrDynamicColumnDescription(String longName, String shortName, ColumnValueDescription desc) {
            this.longName = longName;
            this.shortName = shortName;
            this.desc = desc;
        }
    }
}
