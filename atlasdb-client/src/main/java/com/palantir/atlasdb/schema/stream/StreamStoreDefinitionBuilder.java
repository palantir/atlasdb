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

import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;

public class StreamStoreDefinitionBuilder {
    private final ValueType valueType;
    private final String shortName;
    private final String longName;
    private Map<String, StreamTableDefinitionBuilder> streamTables =
            Maps.newHashMapWithExpectedSize(StreamTableType.values().length);
    private int inMemoryThreshold = AtlasDbConstants.DEFAULT_STREAM_IN_MEMORY_THRESHOLD;
    private boolean compressStream;

    public StreamStoreDefinitionBuilder(String shortName, String longName, ValueType valueType) {
        for (StreamTableType tableType : StreamTableType.values()) {
            streamTables.put(tableType.getTableName(shortName),
                    new StreamTableDefinitionBuilder(tableType, longName, valueType));
        }
        this.valueType = valueType;
        this.shortName = shortName;
        this.longName = longName;
        this.compressStream = false;
    }

    public StreamStoreDefinitionBuilder hashFirstRowComponent() {
        streamTables.forEach((tableName, streamTableBuilder) -> streamTableBuilder.hashFirstRowComponent());
        return this;
    }

    public StreamStoreDefinitionBuilder isAppendHeavyAndReadLight() {
        streamTables.forEach((tableName, streamTableBuilder) -> streamTableBuilder.appendHeavyAndReadLight());
        return this;
    }

    public StreamStoreDefinitionBuilder compressBlocksInDb() {
        streamTables.forEach((tableName, streamTableBuilder) -> streamTableBuilder.compressBlocksInDb());
        return this;
    }

    public StreamStoreDefinitionBuilder compressStreamInClient() {
        compressStream = true;
        return this;
    }

    public StreamStoreDefinitionBuilder inMemoryThreshold(int inMemoryThreshold) {
        this.inMemoryThreshold = inMemoryThreshold;
        return this;
    }

    public StreamStoreDefinition build() {
        Map<String, TableDefinition> tablesToCreate = streamTables.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().build()));

        Preconditions.checkArgument(valueType.getJavaClassName().equals("long"), "Stream ids must be a long");
        Preconditions.checkArgument(inMemoryThreshold <= StreamStoreDefinition.MAX_IN_MEMORY_THRESHOLD,
                "inMemoryThreshold cannot be greater than %s", StreamStoreDefinition.MAX_IN_MEMORY_THRESHOLD);

        return new StreamStoreDefinition(
                tablesToCreate,
                shortName,
                longName,
                valueType,
                inMemoryThreshold,
                compressStream);
    }

}
