/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.containers;

import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

public interface CassandraVersion {
    String CASSANDRA_VERSION = "CASSANDRA_VERSION";
    String DEFAULT_VERSION = "2.2.8";

    static CassandraVersion fromEnvironment() {
        String version = System.getenv(CASSANDRA_VERSION);
        return Strings.isNullOrEmpty(version)
                ? new Cassandra22XVersion()
                : from(version);
    }

    static CassandraVersion from(String version) {
        if (version.startsWith("2.2.")) {
            return new Cassandra22XVersion();
        } else if (version.startsWith("3.")) {
            return new Cassandra3XVersion();
        } else {
            throw new IllegalArgumentException(String.format("Cassandra version %s not supported", version));
        }
    }

    static Map<String, String> getEnvironment() {
        String version = System.getenv(CASSANDRA_VERSION);
        if (Strings.isNullOrEmpty(version)) {
            version = DEFAULT_VERSION;
        }
        return ImmutableMap.of(CASSANDRA_VERSION, version);
    }

    Pattern replicationFactorRegex();

    String getAllKeyspacesCql();
}
