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
package com.palantir.atlasdb.schema.stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.compression.StreamCompression;
import java.util.Map;
import java.util.stream.Collectors;

public class StreamStoreDefinitionBuilder {
    private final ValueType valueType;
    private final String shortName;
    private final String longName;
    private Map<String, StreamTableDefinitionBuilder> streamTables =
            Maps.newHashMapWithExpectedSize(StreamTableType.values().length);
    private int inMemoryThreshold = AtlasDbConstants.DEFAULT_STREAM_IN_MEMORY_THRESHOLD;
    private StreamCompression compressStreamType;
    private int numberOfRowComponentsHashed = 0;

    /**
     * @param shortName The prefix of the table names in the DB.
     * @param longName The prefix of the generated Java class names.
     * @param valueType The type of the column that will store the stream ID internally. Usually a VAR_LONG.
     */
    public StreamStoreDefinitionBuilder(String shortName, String longName, ValueType valueType) {
        for (StreamTableType tableType : StreamTableType.values()) {
            streamTables.put(
                    tableType.getTableName(shortName),
                    new StreamTableDefinitionBuilder(tableType, longName, valueType));
        }
        this.valueType = valueType;
        this.shortName = shortName;
        this.longName = longName;
        this.compressStreamType = StreamCompression.NONE;
    }

    /**
     * We recommend using hashRowComponents() instead as it has the additional benefit of preventing hotspotting
     * within a stream. However, do not change this flag for an existing store schema as we currently do not support
     * StreamStore migrations.
     */
    public StreamStoreDefinitionBuilder hashFirstRowComponent() {
        return hashFirstNRowComponents(1);
    }

    /**
     * We recommend that this flag is set in order to prevent hotspotting in the underlying table which stores
     * the data blocks. The effect of this method is that row keys will be prefixed by the hashed
     * concatenation of the stream id and block id. AtlasDB does not have support for StreamStore migrations,
     * so if you are adding this flag for an existing StreamStore you will have to implement the migration as well.
     */
    public StreamStoreDefinitionBuilder hashRowComponents() {
        return hashFirstNRowComponents(2);
    }

    private StreamStoreDefinitionBuilder hashFirstNRowComponents(int numberOfComponentsHashed) {
        com.palantir.logsafe.Preconditions.checkArgument(
                numberOfComponentsHashed <= 2,
                "The number of components specified must be less than two as "
                        + "StreamStore internal tables use at most two row components.");
        streamTables.forEach((_tableName, streamTableBuilder) ->
                streamTableBuilder.hashFirstNRowComponents(numberOfComponentsHashed));
        numberOfRowComponentsHashed = numberOfComponentsHashed;
        return this;
    }

    public StreamStoreDefinitionBuilder tableNameLogSafety(TableMetadataPersistence.LogSafety logSafety) {
        streamTables.forEach((_tableName, streamTableBuilder) -> streamTableBuilder.tableNameLogSafety(logSafety));
        return this;
    }

    public StreamStoreDefinitionBuilder isAppendHeavyAndReadLight() {
        streamTables.forEach((_tableName, streamTableBuilder) -> streamTableBuilder.appendHeavyAndReadLight());
        return this;
    }

    /**
     * @deprecated use {@link #compressStreamInClient()} instead, because that will compress before sending the data
     * over the network to Cassandra.
     */
    @Deprecated
    public StreamStoreDefinitionBuilder compressBlocksInDb() {
        streamTables.forEach((_tableName, streamTableBuilder) -> streamTableBuilder.compressBlocksInDb());
        return this;
    }

    public StreamStoreDefinitionBuilder compressStreamInClient() {
        return compressStreamInClient(StreamCompression.LZ4);
    }

    public StreamStoreDefinitionBuilder compressStreamInClient(StreamCompression compressionType) {
        compressStreamType = compressionType;
        return this;
    }

    public StreamStoreDefinitionBuilder inMemoryThreshold(int inMemoryThreshold) {
        this.inMemoryThreshold = inMemoryThreshold;
        return this;
    }

    public StreamStoreDefinition build() {
        Map<String, TableDefinition> tablesToCreate = streamTables.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey, entry -> entry.getValue().build()));

        com.palantir.logsafe.Preconditions.checkArgument(
                valueType.getJavaClassName().equals("long"), "Stream ids must be a long");
        Preconditions.checkArgument(
                inMemoryThreshold <= StreamStoreDefinition.MAX_IN_MEMORY_THRESHOLD,
                "inMemoryThreshold cannot be greater than %s",
                StreamStoreDefinition.MAX_IN_MEMORY_THRESHOLD);

        return new StreamStoreDefinition(
                tablesToCreate,
                shortName,
                longName,
                valueType,
                inMemoryThreshold,
                compressStreamType,
                numberOfRowComponentsHashed);
    }
}
