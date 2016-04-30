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

import com.palantir.atlasdb.table.description.render.Renderers;

public enum StreamTableType { // WARNING: do not change these without an upgrade task!
    METADATA("_stream_metadata", "StreamMetadata"),
    VALUE("_stream_value", "StreamValue"),
    HASH("_stream_hash_aidx", "StreamHashAidx"),
    INDEX("_stream_idx", "StreamIdx");

    private final String tableSuffix, javaSuffix;

    private StreamTableType(final String tableSuffix, final String javaSuffix) {
        this.tableSuffix = tableSuffix;
        this.javaSuffix = javaSuffix;
    }

    public String getTableName(String shortPrefix) {
        return shortPrefix + tableSuffix;
    }

    public String getJavaClassName(String prefix) {
        return Renderers.CamelCase(prefix) + javaSuffix;
    }
}
