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

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.migration.generated.MigrationProgressTableFactory;
import com.palantir.atlasdb.migration.generated.ProgressTable;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.Transaction;

public class KvsProgressCheckPointImpl implements ProgressCheckPoint {
    public static final Schema SCHEMA = ProgressCheckpointSchema.INSTANCE.getLatestSchema();
    private static final ProgressTable.ProgressRow ROW = ProgressTable.ProgressRow.of(PtBytes.toBytes("s"));

    public KvsProgressCheckPointImpl(KeyValueService kvs) {
        Schemas.createTable(SCHEMA, kvs, getProgressTable(null).getTableRef());
    }

    @Override
    public Optional<byte[]> getNextStartRow(Transaction transaction) {
        Optional<ProgressTable.ProgressRowResult> result = getProgressTable(transaction).getRow(ROW);

        if (!result.isPresent()) {
            return Optional.of(PtBytes.EMPTY_BYTE_ARRAY);
        }

        if (result.get().getIsDone().equals(1L)) {
            return Optional.empty();
        }

        return Optional.of(result.get().getProgress());
    }

    @Override
    public void setNextStartRow(Transaction transaction, Optional<byte[]> row) {
        ProgressTable progressTable = getProgressTable(transaction);
        if (!row.isPresent()) {
            progressTable.putIsDone(ROW, 1L);
            progressTable.deleteProgress(ROW);
            return;
        }

        progressTable.putIsDone(ROW, 0L);
        progressTable.putProgress(ROW, row.get());
    }

    private ProgressTable getProgressTable(Transaction transaction) {
        return getFactory().getProgressTable(transaction);
    }

    private MigrationProgressTableFactory getFactory() {
        return MigrationProgressTableFactory.of();
    }
}
