/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep.progress;

import java.util.Optional;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.SweepProgressTable;
import com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRow;
import com.palantir.atlasdb.schema.generated.SweepProgressTable.SweepProgressRowResult;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;

public class SweepProgressStore {

    private final KeyValueService kvs;
    private final SweepTableFactory tableFactory;

    public SweepProgressStore(KeyValueService kvs, SweepTableFactory tableFactory) {
        this.kvs = kvs;
        this.tableFactory = tableFactory;
    }

    public Optional<SweepProgress> loadProgress(Transaction tx)  {
        SweepProgressTable progressTable = tableFactory.getSweepProgressTable(tx);
        Optional<SweepProgressRowResult> result = Optional.ofNullable(
                progressTable.getRow(SweepProgressRow.of(0)).orNull());
        return result.map(SweepProgressStore::hydrateProgress);
    }

    public void saveProgress(Transaction tx, SweepProgress progress) {
        SweepProgressTable progressTable = tableFactory.getSweepProgressTable(tx);
        SweepProgressRow row = SweepProgressRow.of(0);
        progressTable.putFullTableName(row, progress.tableRef().getQualifiedName());
        progressTable.putStartRow(row, progress.startRow());
        progressTable.putCellsDeleted(row, progress.cellsDeleted());
        progressTable.putCellsExamined(row, progress.cellsExamined());
        progressTable.putMinimumSweptTimestamp(row, progress.minimumSweptTimestamp());
    }

    /**
     * Fully remove the contents of the sweep progress table.
     */
    public void clearProgress() {
        // Use deleteRange instead of truncate
        // 1) The table should be small, performance difference should be negligible.
        // 2) Truncate takes an exclusive lock in Postgres, which can interfere
        // with concurrently running backups.
        kvs.deleteRange(tableFactory.getSweepProgressTable(null).getTableRef(), RangeRequest.all());
    }

    private static SweepProgress hydrateProgress(SweepProgressTable.SweepProgressRowResult rr) {
        return ImmutableSweepProgress.builder()
                .tableRef(TableReference.createUnsafe(rr.getFullTableName()))
                .startRow(rr.getStartRow())
                .cellsExamined(rr.getCellsExamined())
                .cellsDeleted(rr.getCellsDeleted())
                .minimumSweptTimestamp(rr.getMinimumSweptTimestamp())
                .build();
    }

}
