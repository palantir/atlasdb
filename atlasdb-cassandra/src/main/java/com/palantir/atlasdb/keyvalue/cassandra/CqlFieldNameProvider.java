/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;

public class CqlFieldNameProvider {
    private boolean isScylla;

    public CqlFieldNameProvider(CassandraKeyValueServiceConfig config) {
        isScylla = config.scyllaDb();
    }

    public String row() {
        return isScylla ? "key1" : "key";
    }

    public String column() {
        return "column1";
    }

    public String timestamp() {
        return "column2";
    }

    public String value() {
        return "value";
    }
}
