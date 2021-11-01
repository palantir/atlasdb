/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.example.profile.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.persister.JsonNodePersister;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinitionBuilder;
import com.palantir.atlasdb.table.description.IndexDefinition;
import com.palantir.atlasdb.table.description.IndexDefinition.IndexType;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.example.profile.protos.generated.ProfilePersistence;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
public class ProfileSchema implements AtlasSchema {
    public static final AtlasSchema INSTANCE = new ProfileSchema();

    private static final Schema PROFILE_SCHEMA = generateSchema();

    private static Schema generateSchema() {
        Schema schema = new Schema(
                "Profile",
                ProfileSchema.class.getPackage().getName() + ".generated",
                Namespace.DEFAULT_NAMESPACE,
                OptionalType.JAVA8);

        schema.addTableDefinition("user_profile", new TableDefinition() {
            {
                allSafeForLoggingByDefault();
                rowName();
                rowComponent("id", ValueType.UUID);
                columns();
                column("metadata", "m", ProfilePersistence.UserProfile.class);
                column("create", "c", CreationData.Persister.class);
                column("json", "j", JsonNodePersister.class);
                column("photo_stream_id", "p", ValueType.FIXED_LONG);
            }
        });

        schema.addIndexDefinition("user_birthdays", new IndexDefinition(IndexType.CELL_REFERENCING) {
            {
                onTable("user_profile");
                allSafeForLoggingByDefault();
                rowName();
                componentFromColumn("birthday", ValueType.VAR_SIGNED_LONG, "metadata", "_value.getBirthEpochDay()");
                dynamicColumns();
                componentFromRow("id", ValueType.UUID);
                rangeScanAllowed();
                ignoreHotspottingChecks();
            }
        });

        schema.addIndexDefinition("created", new IndexDefinition(IndexType.CELL_REFERENCING) {
            {
                onTable("user_profile");
                allSafeForLoggingByDefault();
                rowName();
                componentFromColumn("time", ValueType.VAR_LONG, "create", "_value.getTimeCreated()");
                dynamicColumns();
                componentFromRow("id", ValueType.UUID);
                rangeScanAllowed();
                ignoreHotspottingChecks();
            }
        });

        schema.addIndexDefinition("cookies", new IndexDefinition(IndexType.CELL_REFERENCING) {
            {
                onTable("user_profile");
                allSafeForLoggingByDefault();
                rowName();
                componentFromIterableColumn(
                        "cookie",
                        ValueType.STRING,
                        ValueByteOrder.ASCENDING,
                        "json",
                        "com.palantir.example.profile.schema.ProfileSchema.getCookies(_value)");
                dynamicColumns();
                componentFromRow("id", ValueType.UUID);
                rangeScanAllowed();
            }
        });

        schema.addStreamStoreDefinition(
                new StreamStoreDefinitionBuilder("user_photos", "user_photos", ValueType.VAR_LONG)
                        .tableNameLogSafety(TableMetadataPersistence.LogSafety.SAFE)
                        .build());

        return schema;
    }

    public static Iterable<String> getCookies(JsonNode node) {
        JsonNode cookies = node.get("cookies");
        List<String> ret = new ArrayList<>();
        for (JsonNode cookie : cookies) {
            ret.add(cookie.asText());
        }
        return ret;
    }

    public static Schema getSchema() {
        return PROFILE_SCHEMA;
    }

    public static void main(String[] _args) throws Exception {
        PROFILE_SCHEMA.renderTables(new File("src/main/java"));
    }

    @Override
    public Schema getLatestSchema() {
        return PROFILE_SCHEMA;
    }

    @Override
    public Namespace getNamespace() {
        return Namespace.DEFAULT_NAMESPACE;
    }
}
