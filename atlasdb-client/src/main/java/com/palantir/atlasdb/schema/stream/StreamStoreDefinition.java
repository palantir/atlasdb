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

import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.table.description.render.Renderers;
import com.palantir.atlasdb.table.description.render.StreamStoreRenderer;
import com.palantir.common.base.Throwables;

public class StreamStoreDefinition {
    // from ArrayList.MAX_ARRAY_SIZE on 64-bit systems
    public static final int MAX_IN_MEMORY_THRESHOLD = Integer.MAX_VALUE - 8;

    private final Map<String, TableDefinition> streamStoreTables;
    private final String shortName;
    private final String longName;
    private final ValueType idType;
    private final boolean compressStream;

    private int inMemoryThreshold;

    StreamStoreDefinition(
            Map<String, TableDefinition> streamStoreTables,
            String shortName,
            String longName,
            ValueType idType,
            int inMemoryThreshold,
            boolean compressStream) {
        this.streamStoreTables = streamStoreTables;
        this.shortName = shortName;
        this.longName = longName;
        this.idType = idType;
        this.inMemoryThreshold = inMemoryThreshold;
        this.compressStream = compressStream;
    }

    public Map<String, TableDefinition> getTables() {
        return streamStoreTables;
    }

    public StreamStoreRenderer getRenderer(String packageName, String name) {
        String renderedLongName = Renderers.CamelCase(longName);
        return new StreamStoreRenderer(renderedLongName, idType, packageName, name, inMemoryThreshold, compressStream);
    }

    public Multimap<String, Supplier<OnCleanupTask>> getCleanupTasks(
            String packageName,
            String name,
            StreamStoreRenderer renderer,
            Namespace namespace) {
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
