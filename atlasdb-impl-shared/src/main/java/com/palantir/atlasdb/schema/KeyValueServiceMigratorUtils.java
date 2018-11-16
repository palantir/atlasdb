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
package com.palantir.atlasdb.schema;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Set;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator.KvsMigrationMessageLevel;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator.KvsMigrationMessageProcessor;

public final class KeyValueServiceMigratorUtils {

    public static final String CHECKPOINT_TABLE_NAME = "tmp_migrate_progress";

    private KeyValueServiceMigratorUtils() {
        // Utility class
    }

    /**
     * Tables that are eligible for dropping and creating.
     */
    public static Set<TableReference> getCreatableTables(KeyValueService kvs, Set<TableReference> skipTables) {
        /*
         * Tables that cannot be migrated because they are not controlled by the transaction table,
         * but that don't necessarily live on the legacy DB KVS, should still be created on the new
         * KVS, even if they don't get populated. That's why this method is subtly different from
         * getMigratableTableNames().
         */
        Set<TableReference> tableNames = Sets.newHashSet(kvs.getAllTableNames());
        tableNames.removeAll(AtlasDbConstants.ATOMIC_TABLES);
        tableNames.removeAll(skipTables);
        return tableNames;
    }

    /**
     * Tables that are eligible for migration.
     */
    public static Set<TableReference> getMigratableTableNames(KeyValueService kvs, Set<TableReference> skipTables,
            TableReference checkpointTable) {
        /*
         * Not all tables can be migrated. We run by default with a table-splitting KVS that pins
         * certain tables to always be in the legacy DB KVS (because that one supports
         * putUnlessExists), and those tables cannot and should not be migrated. Also, special
         * tables that are not controlled by the transaction table should not be
         * migrated. Lastly, we never migrate any checkpoints from previous KVS migration attempts,
         * since that would corrupt the current migration. Since the namespace might have changed, we
         * remove all table names that match the internal checkpoint table name.
         */
        Set<TableReference> tableNames = getCreatableTables(kvs, skipTables);
        tableNames.removeAll(TargetedSweepSchema.INSTANCE.getLatestSchema().getTableDefinitions().keySet());
        tableNames.removeAll(AtlasDbConstants.HIDDEN_TABLES);
        tableNames.removeIf(tableRef -> tableRef.equals(checkpointTable));
        return tableNames;
    }

    public static void processMessage(
            KvsMigrationMessageProcessor messageProcessor,
            String string,
            KvsMigrationMessageLevel level) {
        messageProcessor.processMessage(string, level);
    }

    public static void processMessage(
            KvsMigrationMessageProcessor messageProcessor,
            String string,
            Throwable ex,
            KvsMigrationMessageLevel level) {
        String outputString = getThrowableMessage(string, ex);
        processMessage(messageProcessor, outputString, level);
    }

    private static String getThrowableMessage(String string, Throwable ex) {
        Writer result = new StringWriter();
        PrintWriter writer = new PrintWriter(result);
        ex.printStackTrace(writer); // (authorized)
        return string + "\n" + result.toString();
    }
}
