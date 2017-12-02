/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.schema.queue;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class PersistentSequentialSchemas {
    private PersistentSequentialSchemas() {
        // utility
    }

    public static void addSequentialTableDefinitions(Schema schema, String sequentialTablePrefix) {
        schema.addTableDefinition(sequentialTablePrefix + "_queue", getSequentialTableDefinition());
        schema.addTableDefinition(sequentialTablePrefix + "_read_offsets", getOffsetTableDefinition());
        schema.addTableDefinition(sequentialTablePrefix + "_write_offsets", getOffsetTableDefinition());
    }

    private static TableDefinition getSequentialTableDefinition() {
        return new TableDefinition() {{
            allSafeForLoggingByDefault();
            rowName();
                rowComponent("queue_key", ValueType.BLOB);
            rangeScanAllowed();
            dynamicColumns();
                columnComponent("offset", ValueType.VAR_LONG);
                value(ValueType.BLOB);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH);
        }};
    }

    private static TableDefinition getOffsetTableDefinition() {
        return new TableDefinition() {{
                    allSafeForLoggingByDefault();
                    conflictHandler(ConflictHandler.SERIALIZABLE);
                    rowName();
                        rowComponent("full_table_name", ValueType.STRING);
                    columns();
                        column("offset", "o", ValueType.VAR_LONG);
                }};
    }
}
