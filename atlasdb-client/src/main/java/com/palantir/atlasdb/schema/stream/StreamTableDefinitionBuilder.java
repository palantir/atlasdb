/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import static java.lang.Math.min;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.stream.GenericStreamStore;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

@SuppressWarnings("checkstyle:all") // too many warnings to fix
public class StreamTableDefinitionBuilder {

    private final StreamTableType streamTableType;
    private final String prefix;
    private final ValueType idType;

    private ExpirationStrategy expirationStrategy = ExpirationStrategy.NEVER;
    private boolean hashFirstRowComponent = false;
    private boolean appendHeavyAndReadLight = false;
    private boolean dbSideCompressionForBlocks = false;
    private int numberOfComponentsHashed = 0;

    public StreamTableDefinitionBuilder(StreamTableType type, String prefix, ValueType idType) {
        this.streamTableType = type;
        this.prefix = prefix;
        this.idType = idType;
    }

    public StreamTableDefinitionBuilder(StreamTableType type, String prefix, ValueType idType, ExpirationStrategy expirationStrategy, boolean hashFirstRowComponent, boolean appendHeavyAndReadLight, boolean dbSideCompressionForBlocks) {
        this.streamTableType = type;
        this.prefix = prefix;
        this.idType = idType;

        this.expirationStrategy = expirationStrategy;
        this.hashFirstRowComponent = hashFirstRowComponent;
        this.appendHeavyAndReadLight = appendHeavyAndReadLight;
        this.dbSideCompressionForBlocks = dbSideCompressionForBlocks;
    }

    public StreamTableDefinitionBuilder expirationStrategy(ExpirationStrategy expirationStrategy) {
        this.expirationStrategy = expirationStrategy;
        return this;
    }

    public StreamTableDefinitionBuilder hashFirstNRowComponents(int numberOfComponentsHashed) {
        Preconditions.checkArgument(numberOfComponentsHashed <= 2,
                "The number of components specified must be less than two as " +
                        "StreamStore internal tables use at most two row components.");
        this.numberOfComponentsHashed = numberOfComponentsHashed;
        return this;
    }

    public StreamTableDefinitionBuilder appendHeavyAndReadLight() {
        appendHeavyAndReadLight = true;
        return this;
    }

    public StreamTableDefinitionBuilder compressBlocksInDb() {
        dbSideCompressionForBlocks = true;
        return this;
    }

    public TableDefinition build() {
        switch(streamTableType) {

        case HASH:
            return new TableDefinition() {{
                javaTableName(streamTableType.getJavaClassName(prefix));
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
                if (appendHeavyAndReadLight) {
                    appendHeavyAndReadLight();
                }
                ignoreHotspottingChecks();
            }};

        case INDEX:
            return new TableDefinition() {{
                javaTableName(streamTableType.getJavaClassName(prefix));
                rowName();
                    // Can hash at most one component for this table.
                    hashFirstNRowComponents(min(numberOfComponentsHashed, 1));
                    rowComponent("id",            idType);
                dynamicColumns();
                    columnComponent("reference", ValueType.SIZED_BLOB);
                    value(ValueType.VAR_LONG);
                conflictHandler(ConflictHandler.IGNORE_ALL);
                maxValueSize(1);
                explicitCompressionRequested();
                expirationStrategy(expirationStrategy);
                if (appendHeavyAndReadLight) {
                    appendHeavyAndReadLight();
                }
                ignoreHotspottingChecks();
        }};

        case METADATA:
            return new TableDefinition() {{
                javaTableName(streamTableType.getJavaClassName(prefix));
                rowName();
                    // Can hash at most one component for this table.
                    hashFirstNRowComponents(min(numberOfComponentsHashed, 1));
                    rowComponent("id", idType);
                columns();
                    column("metadata", "md", StreamPersistence.StreamMetadata.class);
                maxValueSize(64);
                conflictHandler(ConflictHandler.RETRY_ON_VALUE_CHANGED);
                explicitCompressionRequested();
                negativeLookups();
                expirationStrategy(expirationStrategy);
                if (appendHeavyAndReadLight) {
                    appendHeavyAndReadLight();
                }
                ignoreHotspottingChecks();
            }};

        case VALUE:
            return new TableDefinition() {{
                javaTableName(streamTableType.getJavaClassName(prefix));
                rowName();
                    hashFirstNRowComponents(numberOfComponentsHashed);
                    rowComponent("id",              idType);
                    rowComponent("block_id",        ValueType.VAR_LONG);
                columns();
                    column("value", "v",            ValueType.BLOB);
                conflictHandler(ConflictHandler.IGNORE_ALL);
                maxValueSize(GenericStreamStore.BLOCK_SIZE_IN_BYTES);
                cachePriority(CachePriority.COLD);
                expirationStrategy(expirationStrategy);
                if (appendHeavyAndReadLight) {
                    appendHeavyAndReadLight();
                }
                if (dbSideCompressionForBlocks) {
                    explicitCompressionBlockSizeKB(Integer.highestOneBit(GenericStreamStore.BLOCK_SIZE_IN_BYTES / 2));
                }
                ignoreHotspottingChecks();
            }};

        default:
            throw new IllegalStateException("Incorrectly supplied stream table type");
        }
    }
}
