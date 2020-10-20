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

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator.KvsMigrationMessageLevel;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator.KvsMigrationMessageProcessor;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

public final class KeyValueServiceMigratorUtils {

    public static final String CHECKPOINT_TABLE_NAME = "tmp_migrate_progress";

    private KeyValueServiceMigratorUtils() {
        // Utility class
    }

    /**
     * Tables that are eligible for migration. Non-transactional tables and potential old checkpoint tables cannot be
     * migrated since the migrator reads and writes data transactionally, and migrating a checkpoint table would
     * interfere with the migration. We still want to drop and recreate these tables to enable manual migration.
     */
    public static Set<TableReference> getMigratableTableNames(
            KeyValueService kvs, Set<TableReference> skipTables, TableReference checkpointTable) {
        Set<TableReference> tableNames = getCreatableTables(kvs, skipTables);
        tableNames.removeAll(TargetedSweepSchema.INSTANCE
                .getLatestSchema()
                .getTableDefinitions()
                .keySet());
        tableNames.removeAll(AtlasDbConstants.HIDDEN_TABLES);
        tableNames.removeIf(tableRef -> tableRef.equals(checkpointTable));
        return tableNames;
    }

    /**
     * We must not drop and recreate atomic tables or any of the tables that should specifically be skipped. This is
     * because large internal product uses a {@link com.palantir.atlasdb.keyvalue.impl.TableSplittingKeyValueService}
     * that delegates all interactions with these tables to the source KVS as the target KVS, so dropping them would
     * cause us to drop them in the source KVS.
     */
    public static Set<TableReference> getCreatableTables(KeyValueService kvs, Set<TableReference> skipTables) {
        Set<TableReference> tableNames = new HashSet<>(kvs.getAllTableNames());
        tableNames.removeAll(AtlasDbConstants.ATOMIC_TABLES);
        tableNames.removeAll(skipTables);
        return tableNames;
    }

    public static void processMessage(
            KvsMigrationMessageProcessor messageProcessor, String string, KvsMigrationMessageLevel level) {
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
