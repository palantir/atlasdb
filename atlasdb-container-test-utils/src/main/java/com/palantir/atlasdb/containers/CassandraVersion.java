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

import com.google.common.base.Strings;

public enum CassandraVersion {
    VERSION_2_2_X(
            "^.*\"replication_factor\":\"(\\d+)\"\\}$",
            "SELECT * FROM system.schema_keyspaces;"),
    VERSION_3_X(
            "^.*'replication_factor': '(\\d+)'\\}$",
            "SELECT * FROM system_schema.keyspaces;");

    private final String replicationFactorRegex;
    private final String getAllKeyspacesCql;

    CassandraVersion(String replicationFactorRegex, String getAllKeyspacesCql) {
        this.replicationFactorRegex = replicationFactorRegex;
        this.getAllKeyspacesCql = getAllKeyspacesCql;
    }

    public String replicationFactorRegex() {
        return replicationFactorRegex;
    }

    public String getAllKeyspacesCql() {
        return getAllKeyspacesCql;
    }

    public static CassandraVersion fromEnvironment() {
        String version = System.getenv("CASSANDRA_VERSION");
        return Strings.isNullOrEmpty(version)
                ? VERSION_2_2_X
                : from(version);
    }

    public static CassandraVersion from(String version) {
        if (version.startsWith("2.2.")) {
            return VERSION_2_2_X;
        } else if (version.startsWith("3.")) {
            return VERSION_3_X;
        } else {
            throw new IllegalArgumentException(String.format("Cassandra version %s not supported", version));
        }
    }
}
