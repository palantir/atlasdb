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
package com.palantir.atlasdb.schema.stream;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.render.Renderers;
import com.palantir.logsafe.Preconditions;

public enum StreamTableType { // WARNING: do not change these without an upgrade task!
    METADATA("_stream_metadata", "StreamMetadata"),
    VALUE("_stream_value", "StreamValue"),
    HASH("_stream_hash_aidx", "StreamHashAidx"),
    INDEX("_stream_idx", "StreamIdx");

    private final String tableSuffix;
    private final String javaSuffix;

    StreamTableType(final String tableSuffix, final String javaSuffix) {
        this.tableSuffix = tableSuffix;
        this.javaSuffix = javaSuffix;
    }

    public String getTableName(String shortPrefix) {
        return shortPrefix + tableSuffix;
    }

    public String getJavaClassName(String prefix) {
        return Renderers.CamelCase(prefix) + javaSuffix;
    }

    public static boolean isStreamStoreValueTable(TableReference tableReference) {
        return tableReference.getQualifiedName().endsWith(StreamTableType.VALUE.tableSuffix);
    }

    public static TableReference getIndexTableFromValueTable(TableReference tableReference) {
        Preconditions.checkArgument(
                isStreamStoreValueTable(tableReference), "tableReference should be a StreamStore value table");

        int tableNameLastIndex = tableReference.getQualifiedName().lastIndexOf(StreamTableType.VALUE.tableSuffix);
        String indexTableName = tableReference.getQualifiedName().substring(0, tableNameLastIndex) + INDEX.tableSuffix;
        return TableReference.createUnsafe(indexTableName);
    }
}
