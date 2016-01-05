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
package com.palantir.atlasdb.schema.stream;

import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.stream.GenericStreamStore;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.table.description.render.Renderers;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class StreamTables {
    // WARNING: do not change these without an upgrade task!
    public static final String METADATA_TABLE_SUFFIX = "_stream_metadata";
    public static final String VALUE_TABLE_SUFFIX = "_stream_value";
    public static final String HASH_TABLE_SUFFIX = "_stream_hash_idx";
    public static final String INDEX_TABLE_SUFFIX = "_stream_idx";

    public static void generateSchema(Schema schema, final String shortPrefix, final String longPrefix, final ValueType idType) {
        schema.addTableDefinition(shortPrefix + METADATA_TABLE_SUFFIX, getStreamMetadataDefinition(longPrefix, idType, ExpirationStrategy.NEVER, false, false));
        schema.addTableDefinition(shortPrefix + VALUE_TABLE_SUFFIX, getStreamValueDefinition(longPrefix, idType, ExpirationStrategy.NEVER, false, false));
        schema.addTableDefinition(shortPrefix + HASH_TABLE_SUFFIX, getStreamHashIdxDefinition(longPrefix, idType, ExpirationStrategy.NEVER, false));
        schema.addTableDefinition(shortPrefix + INDEX_TABLE_SUFFIX, getStreamIdxDefinition(longPrefix, idType, ExpirationStrategy.NEVER, false, false));
    }

    public static TableDefinition getStreamIdxDefinition(final String longPrefix,
                                                         final ValueType idType,
                                                         final ExpirationStrategy expirationStrategy,
                                                         final boolean hashFirstRowComponent,
                                                         final boolean isAppendHeavyAndReadLight) {

            return new TableDefinition() {{
            javaTableName(Renderers.CamelCase(longPrefix) + "StreamIdx");
            rowName();
                if (hashFirstRowComponent) {
                    hashFirstRowComponent();
                }
                rowComponent("id",            idType);
            dynamicColumns();
                columnComponent("reference", ValueType.SIZED_BLOB);
                value(ValueType.VAR_LONG);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            maxValueSize(1);
            explicitCompressionRequested();
            expirationStrategy(expirationStrategy);
            if (isAppendHeavyAndReadLight) {
                isAppendHeavyAndReadLight();
            }
        }};
    }

    public static TableDefinition getStreamHashIdxDefinition(final String longPrefix,
                                                             final ValueType idType,
                                                             final ExpirationStrategy expirationStrategy,
                                                             final boolean isAppendHeavyAndReadLight) {

        return new TableDefinition() {{
            javaTableName(Renderers.CamelCase(longPrefix) + "StreamHashAidx");
            rowName();
                rowComponent("hash",            ValueType.SHA256HASH);
            dynamicColumns();
                columnComponent("stream_id",    idType);
                value(ValueType.VAR_LONG);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            maxValueSize(1);
            explicitCompressionRequested();
            negativeLookups();
            expirationStrategy(expirationStrategy);
            if (isAppendHeavyAndReadLight) {
                isAppendHeavyAndReadLight();
            }
        }};
    }

    public static TableDefinition getStreamValueDefinition(final String longPrefix,
                                                           final ValueType idType,
                                                           final ExpirationStrategy expirationStrategy,
                                                           final boolean hashFirstRowComponent,
                                                           final boolean isAppendHeavyAndReadLight) {
        return new TableDefinition() {{
            javaTableName(Renderers.CamelCase(longPrefix) + "StreamValue");
            rowName();
                if (hashFirstRowComponent) {
                    hashFirstRowComponent();
                }
                rowComponent("id",              idType);
                rowComponent("block_id",        ValueType.VAR_LONG);
            columns();
                column("value", "v",            ValueType.BLOB);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            maxValueSize(GenericStreamStore.BLOCK_SIZE_IN_BYTES);
            cachePriority(CachePriority.COLD);
            expirationStrategy(expirationStrategy);
            if (isAppendHeavyAndReadLight) {
                isAppendHeavyAndReadLight();
            }
        }};
    }

    public static TableDefinition getStreamMetadataDefinition(final String longPrefix,
                                                              final ValueType idType,
                                                              final ExpirationStrategy expirationStrategy,
                                                              final boolean hashFirstRowComponent,
                                                              final boolean isAppendHeavyAndReadLight) {
        return new TableDefinition() {{
            javaTableName(Renderers.CamelCase(longPrefix) + "StreamMetadata");
            rowName();
                if (hashFirstRowComponent) {
                    hashFirstRowComponent();
                }
                rowComponent("id", idType);
            columns();
                column("metadata", "md", StreamPersistence.StreamMetadata.class);
            maxValueSize(64);
            conflictHandler(ConflictHandler.RETRY_ON_VALUE_CHANGED);
            explicitCompressionRequested();
            negativeLookups();
            expirationStrategy(expirationStrategy);
            if (isAppendHeavyAndReadLight) {
                isAppendHeavyAndReadLight();
            }
        }};
    }
}
