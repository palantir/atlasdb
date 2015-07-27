package com.palantir.example.profile.schema;

import java.io.File;

import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.table.description.IndexDefinition;
import com.palantir.atlasdb.table.description.IndexDefinition.IndexType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.example.profile.protos.generated.ProfilePersistence;

public class ProfileSchema implements AtlasSchema {
    public static final AtlasSchema INSTANCE = new ProfileSchema();

    private static final Schema PROFILE_SCHEMA = generateSchema();

    private static Schema generateSchema() {
        Schema schema = new Schema("Profile",
                ProfileSchema.class.getPackage().getName() + ".generated",
                Namespace.EMPTY_NAMESPACE);

        schema.addTableDefinition("user_profile", new TableDefinition() {{
            rowName();
                rowComponent("id", ValueType.FIXED_LONG);
            columns();
                column("metadata", "m", ProfilePersistence.UserProfile.class);
                column("photo_stream_id", "p", ValueType.FIXED_LONG);
        }});

        schema.addIndexDefinition("user_birthdays", new IndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("user_profile");
            rowName();
                componentFromColumn("birthday", ValueType.VAR_SIGNED_LONG, "metadata", "_value.getBirthEpochDay()");
            dynamicColumns();
                componentFromRow("id", ValueType.FIXED_LONG);
            rangeScanAllowed();
        }});

        schema.addStreamStoreDefinition("user_photos", "user_photos", ValueType.VAR_LONG, 2 * 1024 * 1024);

        return schema;
    }

    public static Schema getSchema() {
        return PROFILE_SCHEMA;
    }

    public static void main(String[]  args) throws Exception {
        PROFILE_SCHEMA.renderTables(new File("src"));
    }

    @Override
    public Schema getLatestSchema() {
        return PROFILE_SCHEMA;
    }

    @Override
    public Namespace getNamespace() {
        return Namespace.EMPTY_NAMESPACE;
    }
}
