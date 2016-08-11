package com.palantir.atlasdb.sql.jdbc.results;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.ValueType;

public class JdbcColumnMetadata {
    static final String DYNAMIC_COLUMN_LABEL = "dyn";

    private final Optional<NameComponentDescription> rowComp;
    private final Optional<NamedOrDynamicColumnDescription> col;

    public static JdbcColumnMetadata create(NameComponentDescription rowComp) {
        return new JdbcColumnMetadata(Optional.of(rowComp), Optional.absent());
    }

    public static JdbcColumnMetadata create(NamedColumnDescription col) {
        return new JdbcColumnMetadata(Optional.absent(),
                Optional.of(new NamedOrDynamicColumnDescription(col.getLongName(), col.getShortName(), col.getValue())));
    }

    public static JdbcColumnMetadata create(DynamicColumnDescription col) {
        return new JdbcColumnMetadata(Optional.absent(),
                Optional.of(new NamedOrDynamicColumnDescription(DYNAMIC_COLUMN_LABEL, DYNAMIC_COLUMN_LABEL, col.getValue())));
    }

    private JdbcColumnMetadata(Optional<NameComponentDescription> rowComp, Optional<NamedOrDynamicColumnDescription> col) {
        Preconditions.checkArgument(rowComp.isPresent() ^ col.isPresent(), "only one description should be present");
        this.rowComp = rowComp;
        this.col = col;
    }

    public boolean isRowComp() {
        return rowComp.isPresent();
    }

    public boolean isCol() {
        return col.isPresent();
    }

    public boolean isNamedCol() {
        return !isDynCol();
    }

    public boolean isDynCol() {
        return isCol() && col.get().longName.equals(DYNAMIC_COLUMN_LABEL);
    }

    public ColumnValueDescription.Format getFormat() {
        return isRowComp() ? ColumnValueDescription.Format.VALUE_TYPE : col.get().desc.getFormat();
    }

    public ValueType getValueType() {
        return isRowComp() ? rowComp.get().getType() : col.get().desc.getValueType();
    }

    public String getLabel() {
        return isRowComp() ? rowComp.get().getComponentName() : col.get().longName;
    }

    public String getName() {
        return isRowComp() ? rowComp.get().getComponentName() : col.get().shortName;
    }

    public Message hydrateProto(byte[] val) {
        if (isCol()) {
            return col.get().desc.hydrateProto(Thread.currentThread().getContextClassLoader(), val);
        } else {
            throw new UnsupportedOperationException("Only columns can contain PROTO");
        }
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
