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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator.KvsMigrationMessageLevel;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator.KvsMigrationMessageProcessor;

public class KeyValueServiceMigrators {
    /**
     * Tables that are eligible for migration.
     */
    public static Set<TableReference> getMigratableTableNames(KeyValueService kvs, Set<TableReference> unmigratableTables) {
        /*
         * Not all tables can be migrated. We run by default with a table-splitting KVS that pins
         * certain tables to always be in the legacy DB KVS (because that one supports
         * putUnlessExists), and those tables cannot and should not be migrated. Also, special
         * tables that are not controlled by the transaction table should not be
         * migrated.
         */
        HashSet<TableReference> tableNames = Sets.newHashSet(kvs.getAllTableNames());
        tableNames.removeAll(AtlasDbConstants.HIDDEN_TABLES);
        tableNames.removeAll(unmigratableTables);
        return tableNames;
    }

    public static void processMessage(KvsMigrationMessageProcessor messageProcessor, String string, KvsMigrationMessageLevel level) {
        messageProcessor.processMessage(string, level);
    }

    public static void processMessage(KvsMigrationMessageProcessor messageProcessor, String string, Throwable t, KvsMigrationMessageLevel level) {
        String outputString = getThrowableMessage(string, t);
        processMessage(messageProcessor, outputString, level);
    }

    private static String getThrowableMessage(String string, Throwable t) {
        Writer result = new StringWriter();
        PrintWriter writer = new PrintWriter(result);
        t.printStackTrace(writer); // (authorized)
        return string + "\n" + result.toString();
    }
}
