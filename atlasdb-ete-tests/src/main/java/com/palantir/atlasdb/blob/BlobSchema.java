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
package com.palantir.atlasdb.blob;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinitionBuilder;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.File;

public class BlobSchema implements AtlasSchema {
    private static final SafeLogger log = SafeLoggerFactory.get(BlobSchema.class);

    private static final Namespace BLOB_NAMESPACE = Namespace.create("blob");
    private static final Schema GENERATED_SCHEMA = generateSchema();

    @Override
    public Namespace getNamespace() {
        return BLOB_NAMESPACE;
    }

    @Override
    public Schema getLatestSchema() {
        return GENERATED_SCHEMA;
    }

    public static Schema getSchema() {
        return GENERATED_SCHEMA;
    }

    private static Schema generateSchema() {
        Schema schema = new Schema(
                BlobSchema.class.getSimpleName(),
                BlobSchema.class.getPackage().getName() + ".generated",
                BLOB_NAMESPACE);

        schema.addStreamStoreDefinition(new StreamStoreDefinitionBuilder("data", "Data", ValueType.VAR_LONG)
                .hashRowComponents()
                .tableNameLogSafety(TableMetadataPersistence.LogSafety.SAFE)
                .build());

        schema.addStreamStoreDefinition(
                new StreamStoreDefinitionBuilder("hotspottyData", "HotspottyData", ValueType.VAR_SIGNED_LONG).build());

        schema.addTableDefinition("auditedData", new TableDefinition() {
            {
                allSafeForLoggingByDefault();
                rowName();
                rowComponent("id", ValueType.FIXED_LONG);
                columns();
                column("data", "d", ValueType.BLOB);
            }
        });

        schema.addCleanupTask("auditedData", () -> (_tx, cells) -> {
            log.info("Deleted data items: [{}]", UnsafeArg.of("cells", cells));
            return false;
        });

        schema.validate();
        return schema;
    }

    public static void main(String[] _args) throws Exception {
        GENERATED_SCHEMA.renderTables(new File("src/main/java"));
    }
}
