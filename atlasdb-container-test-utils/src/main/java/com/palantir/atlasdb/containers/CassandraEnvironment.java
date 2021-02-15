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
package com.palantir.atlasdb.containers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public final class CassandraEnvironment {
    public static final String CASSANDRA_VERSION = "CASSANDRA_VERSION";

    @VisibleForTesting
    static final String CASSANDRA_MAX_HEAP_SIZE = "CASSANDRA_MAX_HEAP_SIZE";

    @VisibleForTesting
    static final String CASSANDRA_HEAP_NEWSIZE = "CASSANDRA_HEAP_NEWSIZE";

    @VisibleForTesting
    static final String DEFAULT_VERSION = "atlasdb-testing-palantir-cassandra";

    @VisibleForTesting
    static final String DEFAULT_MAX_HEAP_SIZE = "512m";

    @VisibleForTesting
    static final String DEFAULT_HEAP_NEWSIZE = "64m";

    private CassandraEnvironment() {
        // uninstantiable
    }

    public static Map<String, String> get() {
        return ImmutableMap.of(
                CASSANDRA_VERSION, getOrDefault(CASSANDRA_VERSION, DEFAULT_VERSION),
                CASSANDRA_MAX_HEAP_SIZE, getOrDefault(CASSANDRA_MAX_HEAP_SIZE, DEFAULT_MAX_HEAP_SIZE),
                CASSANDRA_HEAP_NEWSIZE, getOrDefault(CASSANDRA_HEAP_NEWSIZE, DEFAULT_HEAP_NEWSIZE));
    }

    public static String getVersion() {
        return getOrDefault(CASSANDRA_VERSION, DEFAULT_VERSION);
    }

    private static String getOrDefault(String name, String defaultValue) {
        String version = System.getenv(name);
        if (Strings.isNullOrEmpty(version)) {
            version = defaultValue;
        }
        return version;
    }
}
