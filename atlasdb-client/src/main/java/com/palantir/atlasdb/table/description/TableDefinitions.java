package com.palantir.atlasdb.table.description;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class TableDefinitions {
    private static final TableMetadata RAW_METADATA = new TableMetadata(
            new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("row", ValueType.BLOB))),
            new ColumnMetadataDescription(new DynamicColumnDescription(new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("row", ValueType.BLOB))), ColumnValueDescription.forType(ValueType.BLOB))),
            ConflictHandler.SERIALIZABLE);

    private static final TableMetadata JSON_METADATA = new TableMetadata(
            new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("row", ValueType.STRING))),
            new ColumnMetadataDescription(new DynamicColumnDescription(new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("c", ValueType.STRING))), ColumnValueDescription.forType(ValueType.BLOB))),
            ConflictHandler.SERIALIZABLE);

    private static final TableMetadata singleColumn(String colName) {
        return new TableMetadata(
                new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("row", ValueType.STRING))),
                new ColumnMetadataDescription(new DynamicColumnDescription(new NameMetadataDescription(ImmutableList.of(new NameComponentDescription(colName, ValueType.STRING))), ColumnValueDescription.forType(ValueType.BLOB))),
                ConflictHandler.SERIALIZABLE);
    }
}
