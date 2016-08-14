package com.palantir.atlasdb.sql.jdbc.results;

public class SelectableJdbcColumnMetadata {

    private final JdbcColumnMetadata metadata;
    private final boolean selected;

    public SelectableJdbcColumnMetadata(JdbcColumnMetadata metadata, boolean selected) {
        this.metadata = metadata;
        this.selected = selected;
    }

    public JdbcColumnMetadata getMetadata() {
        return metadata;
    }

    public boolean isSelected() {
        return selected;
    }
    
}
