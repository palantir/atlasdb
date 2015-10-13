/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.schema;

import java.io.File;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.annotations.PtMain;
import com.palantir.annotations.PtMainType;
import com.palantir.atlasdb.protos.generated.UpgradePersistence;
import com.palantir.atlasdb.table.description.CodeGeneratingSchema;
import com.palantir.atlasdb.table.description.CodeGeneratingTableDefinition;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

/**
 * Defines the schema for maintaining upgrades.
 */
@PtMain(type=PtMainType.DEV)
public enum UpgradeSchema implements AtlasSchema {
    INSTANCE;

    private static final Namespace NAMESPACE = Namespace.create("upgrade");
    private static final Supplier<CodeGeneratingSchema> SCHEMA = Suppliers.memoize(new Supplier<CodeGeneratingSchema>() {
        @Override
        public CodeGeneratingSchema get() {
            return generateSchema();
        }
    });

    private static CodeGeneratingSchema generateSchema() {
        CodeGeneratingSchema schema = new CodeGeneratingSchema("Upgrade",
                UpgradeSchema.class.getPackage().getName() + ".generated",
                Namespace.create("upgrade"));

        schema.addTableDefinition("upgrade_metadata", new CodeGeneratingTableDefinition() {{
            rowName();
                rowComponent("namespace",      ValueType.STRING); partition();
            columns();
                column("status", "s",          ValueType.VAR_LONG);
                column("current_version", "c", UpgradePersistence.SchemaVersion.class);
                column("running_tasks", "r",   UpgradePersistence.SchemaVersions.class);
                column("finished_tasks", "f",  UpgradePersistence.SchemaVersions.class);
            conflictHandler(ConflictHandler.IGNORE_ALL);
        }});

        schema.addTableDefinition("upg_task_metadata", new CodeGeneratingTableDefinition() {{
            rowName();
                rowComponent("namespace",      ValueType.VAR_STRING); partition();
                rowComponent("version",        ValueType.VAR_LONG);
                rowComponent("hotfix_version", ValueType.VAR_LONG);
                rowComponent("hotfix_hotfix",  ValueType.VAR_LONG);
                rowComponent("extra_id",       ValueType.VAR_STRING);
                rowComponent("range_id",       ValueType.VAR_LONG);
            columns();
                column("start", "s",           ValueType.BLOB);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            rangeScanAllowed();
        }});

        schema.validate();
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

    public static void main(String[] args) throws Exception {
        SCHEMA.get().renderTables(new File("src/main/java"));
    }
}
