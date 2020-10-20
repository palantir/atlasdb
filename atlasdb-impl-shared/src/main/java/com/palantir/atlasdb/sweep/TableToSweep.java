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
package com.palantir.atlasdb.sweep;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.lock.SingleLockService;
import java.util.Optional;

public final class TableToSweep {
    private final TableReference tableRef;
    private final SingleLockService sweepLock;
    private boolean hasPreviousProgress;
    private SweepResults previousResults;

    public static TableToSweep newTable(TableReference tableRef, SingleLockService sweepLockForTable) {
        return new TableToSweep(tableRef, sweepLockForTable, Optional.empty());
    }

    public static TableToSweep continueSweeping(
            TableReference tableRef, SingleLockService sweepLock, SweepProgress progress) {
        return new TableToSweep(tableRef, sweepLock, Optional.of(progress));
    }

    private TableToSweep(TableReference tableRef, SingleLockService sweepLock, Optional<SweepProgress> progress) {
        this.tableRef = tableRef;
        this.sweepLock = sweepLock;
        this.hasPreviousProgress = progress.isPresent();
        this.previousResults = progress.map(SweepProgress::getPreviousResults)
                .orElseGet(SweepResults::createEmptySweepResultWithMoreToSweep);
    }

    public TableReference getTableRef() {
        return tableRef;
    }

    public SingleLockService getSweepLock() {
        return sweepLock;
    }

    public void refreshLock() throws InterruptedException {
        sweepLock.lockOrRefresh();
    }

    boolean hasPreviousProgress() {
        return hasPreviousProgress;
    }

    public byte[] getStartRow() {
        return previousResults.getNextStartRow().orElse(PtBytes.EMPTY_BYTE_ARRAY);
    }

    SweepResults getPreviousSweepResults() {
        return previousResults;
    }

    public void setProgress(SweepProgress progress) {
        this.hasPreviousProgress = true;
        this.previousResults = progress.getPreviousResults();
    }
}
