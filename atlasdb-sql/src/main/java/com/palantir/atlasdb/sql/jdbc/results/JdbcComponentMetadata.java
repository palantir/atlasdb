package com.palantir.atlasdb.sql.jdbc.results;

import com.google.protobuf.Message;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.ValueType;

public final class JdbcComponentMetadata {
    abstract static class Component implements JdbcColumnMetadata {
        private final NameComponentDescription comp;

        public Component(NameComponentDescription comp) {
            this.comp = comp;
        }

        @Override
        public ColumnValueDescription.Format getFormat() {
            return ColumnValueDescription.Format.VALUE_TYPE;
        }

        @Override
        public ValueType getValueType() {
            return comp.getType();
        }

        @Override
        public String getLabel() {
            return comp.getComponentName();
        }

        @Override
        public String getName() {
            return comp.getComponentName();
        }

        @Override
        public Message hydrateProto(byte[] val) {
            throw new UnsupportedOperationException("Only columns can contain protobufs protocol buffer.");
        }

        @Override
        public boolean isNamedCol() {
            return false;
        }

        @Override
        public boolean isValueCol() {
            return false;
        }
    }

    public static class NamedCol implements JdbcColumnMetadata {
        private final NamedColumnDescription namedCol;

        public NamedCol(NamedColumnDescription namedCol) {
            this.namedCol = namedCol;
        }

        @Override
        public ColumnValueDescription.Format getFormat() {
            return namedCol.getValue().getFormat();
        }

        @Override
        public ValueType getValueType() {
            return namedCol.getValue().getValueType();
        }

        @Override
        public String getLabel() {
            return namedCol.getLongName();
        }

        @Override
        public String getName() {
            return namedCol.getShortName();
        }

        @Override
        public Message hydrateProto(byte[] val) {
            return namedCol.getValue().hydrateProto(Thread.currentThread().getContextClassLoader(), val);
        }

        @Override
        public boolean isNamedCol() {
            return true;
        }

        @Override
        public boolean isRowComp() {
            return false;
        }

        @Override
        public boolean isColComp() {
            return false;
        }

        @Override
        public boolean isValueCol() {
            return false;
        }
    }

    public static class ValueCol implements JdbcColumnMetadata {
        public static final String VALUE_COLUMN_LABEL = "value";

        private final ColumnValueDescription valDesc;

        public ValueCol(ColumnValueDescription valDesc) {
            this.valDesc = valDesc;
        }

        @Override
        public String getLabel() {
            return VALUE_COLUMN_LABEL;
        }

        @Override
        public String getName() {
            return VALUE_COLUMN_LABEL;
        }

        @Override
        public ColumnValueDescription.Format getFormat() {
            return valDesc.getFormat();
        }

        @Override
        public ValueType getValueType() {
            return valDesc.getValueType();
        }

        @Override
        public Message hydrateProto(byte[] val) {
            return valDesc.hydrateProto(Thread.currentThread().getContextClassLoader(), val);
        }

        @Override
        public boolean isNamedCol() {
            return false;
        }

        @Override
        public boolean isRowComp() {
            return false;
        }

        @Override
        public boolean isColComp() {
            return false;
        }

        @Override
        public boolean isValueCol() {
            return true;
        }
    }

    public static class RowComp extends JdbcComponentMetadata.Component {
        public RowComp(NameComponentDescription comp) {
            super(comp);
        }

        @Override
        public boolean isRowComp() {
            return true;
        }

        @Override
        public boolean isColComp() {
            return false;
        }
    }

    public static class ColComp extends JdbcComponentMetadata.Component {
        public ColComp(NameComponentDescription comp) {
            super(comp);
        }

        @Override
        public boolean isRowComp() {
            return false;
        }

        @Override
        public boolean isColComp() {
            return true;
        }

        @Override
        public boolean isNamedCol() {
            return false;
        }
    }
}
