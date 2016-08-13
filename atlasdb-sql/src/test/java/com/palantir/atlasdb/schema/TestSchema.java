package com.palantir.atlasdb.schema;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TestPersistence;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public enum TestSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace NAMESPACE = Namespace.create("test");
    public static final TableReference ONLY_TABLE =  TableReference.create(NAMESPACE, "only_table");
    public static final TableReference DYNAMIC_COLUMN_TABLE =  TableReference.create(NAMESPACE, "dynamic_table");
    private static final Supplier<Schema> SCHEMA = Suppliers.memoize(TestSchema::generateSchema);

    private static Schema generateSchema() {
        Schema schema = new Schema("Test",
                TestSchema.class.getPackage().getName() + ".generated",
                NAMESPACE);

        /* Schema definition start */
        schema.addTableDefinition(ONLY_TABLE.getTablename(), new TableDefinition() {
            {
                rowName();
                    rowComponent("object_id", ValueType.STRING);
                columns();
                    column("base_object", "b", TestPersistence.TestObject.class, ColumnValueDescription.Compression.NONE);
            }});

        schema.addTableDefinition(DYNAMIC_COLUMN_TABLE.getTablename(), new TableDefinition() {
            {
                rowName();
                    rowComponent("rowComp1",    ValueType.FIXED_LONG);
                    rowComponent("rowComp2",    ValueType.STRING);
                dynamicColumns();
                    columnComponent("colComp1", ValueType.FIXED_LONG);
                    columnComponent("colComp2", ValueType.STRING);
                    value(TestPersistence.TestObject.class);
                conflictHandler(ConflictHandler.IGNORE_ALL);
            }});

        /* Schema definition end */

        schema.validate(); // ensure that the schema as constructed is valid.
        return schema;
    }

    @Override
    public Schema getLatestSchema() {
        return SCHEMA.get();
    }

    @Override
    public Namespace getNamespace() {
        return NAMESPACE;
    }

}
