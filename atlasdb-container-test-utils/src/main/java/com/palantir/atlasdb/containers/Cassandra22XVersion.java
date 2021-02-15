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

import java.util.regex.Pattern;

public class Cassandra22XVersion implements CassandraVersion {
    private static final Pattern REPLICATION_REGEX = Pattern.compile("^.*\"replication_factor\":\"(\\d+)\"\\}$");
    private static final String ALL_KEYSPACES_CQL = "SELECT * FROM system.schema_keyspaces;";

    @Override
    public Pattern replicationFactorRegex() {
        return REPLICATION_REGEX;
    }

    @Override
    public String getAllKeyspacesCql() {
        return ALL_KEYSPACES_CQL;
    }
}
