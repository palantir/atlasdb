/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async.query.forming;

enum SupportedQuery {
    TIME("TIME", "SELECT dateof(now()) FROM system.local;"),
    GET("GET", "SELECT value, column2 FROM %s "
            + "WHERE key = :row AND column1 = :column AND column2 > :timestamp;");

    private final String name;
    private final String format;

    SupportedQuery(String name, String format) {
        this.name = name;
        this.format = format;
    }

    String formQueryString(String fullyQualifiedName) {
        return String.format(this.format, fullyQualifiedName);
    }

    @Override
    public String toString() {
        return "Supported query:" + name;
    }
}
