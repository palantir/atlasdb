/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

public interface TagConsumer extends BiConsumer<String, String> {
    default void tableRef(TableReference tableReference) {
        accept("table", LoggingArgs.safeTableOrPlaceholder(tableReference).toString());
    }

    default void tableRefs(Collection<TableReference> tableReferences) {
        accept("tables", LoggingArgs.safeTablesOrPlaceholder(tableReferences).toString());
    }

    default void timestamp(long ts) {
        accept("ts", Long.toString(ts));
    }

    default void size(@CompileTimeConstant final String name, Iterable<?> iterable) {
        integer(name, Iterables.size(iterable));
    }

    default void size(@CompileTimeConstant final String name, Collection<?> collection) {
        integer(name, collection.size());
    }

    default void size(@CompileTimeConstant final String name, Map<?, ?> map) {
        integer(name, map.size());
    }

    default void size(@CompileTimeConstant final String name, Multimap<?, ?> multiMap) {
        integer(name, multiMap.size());
    }

    default void integer(@CompileTimeConstant final String name, int value) {
        accept(name, Integer.toString(value));
    }

    default void longValue(@CompileTimeConstant final String name, long value) {
        accept(name, Long.toString(value));
    }

    default void statistics(TraceStatistic statistics) {
        long emptyReads = statistics.emptyReads();
        if (emptyReads != 0) {
            longValue("atlasdb.emptyReads", emptyReads);
        }

        long bytesReadFromDb = statistics.bytesReadFromDb();
        if (bytesReadFromDb != 0) {
            longValue("atlasdb.bytesRead", bytesReadFromDb);
        }
    }
}
