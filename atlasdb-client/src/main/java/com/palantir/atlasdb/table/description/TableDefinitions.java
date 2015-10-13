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
package com.palantir.atlasdb.table.description;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class TableDefinitions {
    private static final TableMetadata RAW_METADATA = new DefaultTableMetadata(
            new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("row", ValueType.BLOB))),
            new ColumnMetadataDescription(new DynamicColumnDescription(new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("row", ValueType.BLOB))), ColumnValueDescription.forType(ValueType.BLOB))),
            ConflictHandler.SERIALIZABLE);

    private static final TableMetadata JSON_METADATA = new DefaultTableMetadata(
            new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("row", ValueType.STRING))),
            new ColumnMetadataDescription(new DynamicColumnDescription(new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("c", ValueType.STRING))), ColumnValueDescription.forType(ValueType.BLOB))),
            ConflictHandler.SERIALIZABLE);

    private static final TableMetadata singleColumn(String colName) {
        return new DefaultTableMetadata(
                new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("row", ValueType.STRING))),
                new ColumnMetadataDescription(new DynamicColumnDescription(new NameMetadataDescription(ImmutableList.of(new NameComponentDescription(colName, ValueType.STRING))), ColumnValueDescription.forType(ValueType.BLOB))),
                ConflictHandler.SERIALIZABLE);
    }
}
