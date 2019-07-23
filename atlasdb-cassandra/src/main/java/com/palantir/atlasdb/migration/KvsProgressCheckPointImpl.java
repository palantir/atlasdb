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

package com.palantir.atlasdb.migration;

import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;

public class KvsProgressCheckPointImpl implements ProgressCheckPoint {
    private static final String ROW_AND_COLUMN_NAME = "s";
    private static final TableMetadata PROGRESS_METADATA = TableMetadata.internal()
            .singleRowComponent("dummy", ValueType.STRING)
            .singleNamedColumn(ROW_AND_COLUMN_NAME, "sweep_progress", ValueType.BLOB)
            .build();
    private static final Cell PROGRESS_CELL = Cell.create(PtBytes.toBytes(ROW_AND_COLUMN_NAME),
            PtBytes.toBytes(ROW_AND_COLUMN_NAME));

    private final KeyValueService kvs;

    public KvsProgressCheckPointImpl(KeyValueService kvs) {
        this.kvs = kvs;
        kvs.createTable(AtlasDbConstants.LIVE_MIGRATON_PROGRESS_TABLE, PROGRESS_METADATA.persistToBytes());
    }

    @Override
    public Optional<byte[]> getNextStartRow() {
        return Optional.ofNullable(
                kvs.get(AtlasDbConstants.LIVE_MIGRATON_PROGRESS_TABLE, ImmutableMap.of(PROGRESS_CELL, 1L))
                        .getOrDefault(PROGRESS_CELL, null))
                .map(Value::getContents);
    }

    @Override
    public void setNextStartRow(Optional<byte[]> row) {
        kvs.put(
                AtlasDbConstants.LIVE_MIGRATON_PROGRESS_TABLE,
                ImmutableMap.of(PROGRESS_CELL, row.orElse(PtBytes.EMPTY_BYTE_ARRAY)),
                0L);
    }
}
