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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.table.description.render.Renderers;
import com.palantir.atlasdb.table.description.render.StreamStoreRenderer;
import com.palantir.common.base.Throwables;
import com.palantir.common.compression.StreamCompression;
import java.util.Map;
import java.util.function.Supplier;

public class StreamStoreDefinition {
    // from ArrayList.MAX_ARRAY_SIZE on 64-bit systems
    public static final int MAX_IN_MEMORY_THRESHOLD = Integer.MAX_VALUE - 8;

    private final Map<String, TableDefinition> streamStoreTables;
    private final String shortName;
    private final String longName;
    private final ValueType idType;
    private final StreamCompression streamCompression;
    private final int numberOfRowComponentsHashed;

    private int inMemoryThreshold;

    StreamStoreDefinition(
            Map<String, TableDefinition> streamStoreTables,
            String shortName,
            String longName,
            ValueType idType,
            int inMemoryThreshold,
            StreamCompression streamCompression,
            int numberOfRowComponentsHashed) {
        this.streamStoreTables = streamStoreTables;
        this.shortName = shortName;
        this.longName = longName;
        this.idType = idType;
        this.inMemoryThreshold = inMemoryThreshold;
        this.streamCompression = streamCompression;
        this.numberOfRowComponentsHashed = numberOfRowComponentsHashed;
    }

    public Map<String, TableDefinition> getTables() {
        return streamStoreTables;
    }

    public ValueType getIdType() {
        return idType;
    }

    public int getNumberOfRowComponentsHashed() {
        return numberOfRowComponentsHashed;
    }

    public StreamStoreRenderer getRenderer(String packageName, String name) {
        String renderedLongName = Renderers.CamelCase(longName);
        return new StreamStoreRenderer(
                renderedLongName, idType, packageName, name, inMemoryThreshold, streamCompression);
    }

    public Multimap<String, Supplier<OnCleanupTask>> getCleanupTasks(
            String packageName, String _name, StreamStoreRenderer renderer, Namespace namespace) {
        Multimap<String, Supplier<OnCleanupTask>> cleanupTasks = ArrayListMultimap.create();

        // We use reflection and wrap these in suppliers because these classes are generated classes that
        // might not always exist.
        cleanupTasks.put(StreamTableType.METADATA.getTableName(shortName), () -> {
            try {
                Class<?> clazz = Class.forName(packageName + "." + renderer.getMetadataCleanupTaskClassName());
                try {
                    return (OnCleanupTask) clazz.getConstructor(Namespace.class).newInstance(namespace);
                } catch (Exception e) {
                    return (OnCleanupTask) clazz.getConstructor().newInstance();
                }
            } catch (Exception e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        });

        cleanupTasks.put(StreamTableType.INDEX.getTableName(shortName), () -> {
            try {
                Class<?> clazz = Class.forName(packageName + "." + renderer.getIndexCleanupTaskClassName());
                try {
                    return (OnCleanupTask) clazz.getConstructor(Namespace.class).newInstance(namespace);
                } catch (Exception e) {
                    return (OnCleanupTask) clazz.getConstructor().newInstance();
                }
            } catch (Exception e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        });

        return cleanupTasks;
    }
}
