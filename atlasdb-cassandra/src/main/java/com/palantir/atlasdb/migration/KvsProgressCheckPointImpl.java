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
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;

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
        createTable(kvs);
    }

    @Override
    public Optional<byte[]> getNextStartRow(Transaction transaction) {
        return Optional.ofNullable(
                transaction.get(AtlasDbConstants.LIVE_MIGRATON_PROGRESS_TABLE, ImmutableSet.of(PROGRESS_CELL))
                        .getOrDefault(PROGRESS_CELL, null));
    }

    @Override
    public void setNextStartRow(Transaction transaction, Optional<byte[]> row) {
        transaction.put(AtlasDbConstants.LIVE_MIGRATON_PROGRESS_TABLE,
                ImmutableMap.of(PROGRESS_CELL, row.orElse(PtBytes.EMPTY_BYTE_ARRAY)));
    }

    private void createTable(KeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.LIVE_MIGRATON_PROGRESS_TABLE,
                new TableDefinition() {{
                    rowName();
                    rowComponent("row", ValueType.BLOB);
                    columns();
                    column("col", "c", ValueType.BLOB);
                    conflictHandler(ConflictHandler.SERIALIZABLE_CELL);
                    sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH);
                }}.toTableMetadata().persistToBytes()
        );
    }
}
