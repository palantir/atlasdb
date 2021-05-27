/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.tracing;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.TagTranslator;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface Tracing {

    static CloseableTracer startLocalTrace(@CompileTimeConstant String operation, Consumer<TagConsumer> tagTranslator) {
        return CloseableTracer.startSpan(operation, FunctionalTagTranslator.INSTANCE, tagTranslator);
    }

    interface TagConsumer extends BiConsumer<String, String> {
        default void tableRef(TableReference tableReference) {
            accept("table", LoggingArgs.safeTableOrPlaceholder(tableReference).toString());
        }

        default void timestamp(long ts) {
            accept("ts", Long.toString(ts));
        }

        default void size(@CompileTimeConstant String name, Iterable<?> iterable) {
            integer(name, Iterables.size(iterable));
        }

        default void size(@CompileTimeConstant String name, Collection<?> collection) {
            integer(name, collection.size());
        }

        default void size(@CompileTimeConstant String name, Map<?, ?> map) {
            integer(name, map.size());
        }

        default void size(@CompileTimeConstant String name, Multimap<?, ?> multiMap) {
            integer(name, multiMap.size());
        }

        default void integer(@CompileTimeConstant String name, int value) {
            accept(name, Integer.toString(value));
        }
    }

    enum FunctionalTagTranslator implements TagTranslator<Consumer<TagConsumer>> {
        INSTANCE;

        @Override
        public <T> void translate(TagAdapter<T> adapter, T target, Consumer<TagConsumer> data) {
            data.accept((key, value) -> adapter.tag(target, key, value));
        }
    }
}
