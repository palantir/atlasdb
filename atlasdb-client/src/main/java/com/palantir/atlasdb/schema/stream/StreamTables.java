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
import com.palantir.atlasdb.table.description.CodeGeneratingSchema;
import com.palantir.atlasdb.table.description.CodeGeneratingTableDefinition;
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

    public static void generateSchema(CodeGeneratingSchema schema, final String shortPrefix, final String longPrefix, final ValueType idType) {
        schema.addTableDefinition(shortPrefix + METADATA_TABLE_SUFFIX, getStreamMetadataDefinition(longPrefix, idType, ExpirationStrategy.NEVER));
        schema.addTableDefinition(shortPrefix + VALUE_TABLE_SUFFIX, getStreamValueDefinition(longPrefix, idType, ExpirationStrategy.NEVER));
        schema.addTableDefinition(shortPrefix + HASH_TABLE_SUFFIX, getStreamHashIdxDefinition(longPrefix, idType, ExpirationStrategy.NEVER));
        schema.addTableDefinition(shortPrefix + INDEX_TABLE_SUFFIX, getStreamIdxDefinition(longPrefix, idType, ExpirationStrategy.NEVER));
    }

    public static CodeGeneratingTableDefinition getStreamIdxDefinition(final String longPrefix,
                                                         final ValueType idType,
                                                         final ExpirationStrategy expirationStrategy) {
        return new CodeGeneratingTableDefinition() {{
            javaTableName(Renderers.CamelCase(longPrefix) + "StreamIdx");
            rowName();
                rowComponent("id",            idType);
            dynamicColumns();
                columnComponent("reference", ValueType.SIZED_BLOB);
                value(ValueType.VAR_LONG);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            maxValueSize(1);
            dbCompressionRequested();
            expirationStrategy(expirationStrategy);
        }};
    }

    public static CodeGeneratingTableDefinition getStreamHashIdxDefinition(final String longPrefix,
                                                              final ValueType idType,
                                                              final ExpirationStrategy expirationStrategy) {
        return new CodeGeneratingTableDefinition() {{
            javaTableName(Renderers.CamelCase(longPrefix) + "StreamHashAidx");
            rowName();
                rowComponent("hash",            ValueType.SHA256HASH);
            dynamicColumns();
                columnComponent("stream_id",    idType);
                value(ValueType.VAR_LONG);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            maxValueSize(1);
            dbCompressionRequested();
            negativeLookups();
            expirationStrategy(expirationStrategy);
        }};
    }

    public static CodeGeneratingTableDefinition getStreamValueDefinition(String longPrefix,
                                                           ValueType idType,
                                                           ExpirationStrategy expirationStrategy) {
        return getValueDefinition(longPrefix, idType, "Stream", expirationStrategy);
    }

    public static CodeGeneratingTableDefinition getStreamMetadataDefinition(String longPrefix,
                                                              ValueType idType,
                                                              ExpirationStrategy expirationStrategy) {
        return getMetadataDefinition(longPrefix, idType, "Stream", expirationStrategy);
    }

    public static TableDefinition getBlobValueDefinition(String longPrefix,
                                                         ValueType idType,
                                                         ExpirationStrategy expirationStrategy) {
        return getValueDefinition(longPrefix, idType, "Blob", expirationStrategy);
    }

    public static TableDefinition getBlobMetadataDefinition(String longPrefix,
                                                            ValueType idType,
                                                            ExpirationStrategy expirationStrategy) {
        return getMetadataDefinition(longPrefix, idType, "Blob", expirationStrategy);
    }

    private static CodeGeneratingTableDefinition getValueDefinition(final String longPrefix,
                                                      final ValueType idType,
                                                      final String type,
                                                      final ExpirationStrategy expirationStrategy) {
        return new CodeGeneratingTableDefinition() {{
            javaTableName(Renderers.CamelCase(longPrefix) + type + "Value");
            rowName();
                rowComponent("id",              idType);
                rowComponent("block_id",        ValueType.VAR_LONG);
            columns();
                column("value", "v",            ValueType.BLOB);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            maxValueSize(GenericStreamStore.BLOCK_SIZE_IN_BYTES);
            cachePriority(CachePriority.COLD);
            expirationStrategy(expirationStrategy);
        }};
    }

    private static CodeGeneratingTableDefinition getMetadataDefinition(final String longPrefix,
                                                         final ValueType idType,
                                                         final String type,
                                                         final ExpirationStrategy expirationStrategy) {
        return new CodeGeneratingTableDefinition() {{
            javaTableName(Renderers.CamelCase(longPrefix) + type + "Metadata");
            rowName();
                rowComponent("id",              idType);
            columns();
                column("metadata", "md",        StreamPersistence.StreamMetadata.class);
            maxValueSize(64);
            conflictHandler(ConflictHandler.RETRY_ON_VALUE_CHANGED);
            dbCompressionRequested();
            negativeLookups();
            expirationStrategy(expirationStrategy);
        }};
    }
}
