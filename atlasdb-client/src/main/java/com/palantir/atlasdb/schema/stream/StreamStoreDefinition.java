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
    private final Map<String, TableDefinition> streamStoreTables;
    private final String shortName, longName;
    private final ValueType idType;
    private final boolean clientSideCompression;

    private int inMemoryThreshold;

    StreamStoreDefinition(Map<String, TableDefinition> streamStoreTables, String shortName, String longName, ValueType idType, int inMemoryThreshold, boolean compressStreamInClient) {
        this.streamStoreTables = streamStoreTables;
        this.shortName = shortName;
        this.longName = longName;
        this.idType = idType;
        this.inMemoryThreshold = inMemoryThreshold;
        this.clientSideCompression = compressStreamInClient;
    }

    public Map<String, TableDefinition> getTables() {
        return streamStoreTables;
    }

    public StreamStoreRenderer getRenderer(String packageName, String name) {
        return new StreamStoreRenderer(Renderers.CamelCase(longName), idType, packageName, name, inMemoryThreshold, clientSideCompression);
    }

    public Multimap<String, Supplier<OnCleanupTask>> getCleanupTasks(String packageName, String name, StreamStoreRenderer renderer, Namespace namespace) {
        Multimap<String, Supplier<OnCleanupTask>> cleanupTasks = ArrayListMultimap.create();

        // We use reflection and wrap these in suppliers because these classes are generated classes that might not always exist.
        cleanupTasks.put(StreamTableType.METADATA.getTableName(shortName), new Supplier<OnCleanupTask>() {
            @Override
            public OnCleanupTask get() {
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
            }
        });

        cleanupTasks.put(StreamTableType.INDEX.getTableName(shortName), new Supplier<OnCleanupTask>() {
            @Override
            public OnCleanupTask get() {
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
            }
        });

        return cleanupTasks;
    }
}
