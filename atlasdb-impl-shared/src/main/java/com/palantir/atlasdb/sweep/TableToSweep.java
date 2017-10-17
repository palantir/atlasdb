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

package com.palantir.atlasdb.sweep;

import java.util.Optional;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.progress.SweepProgress;

public final class TableToSweep {
    private final TableReference tableRef;
    private final Optional<SweepProgress> progress;

    TableToSweep(TableReference tableRef, Optional<SweepProgress> progress) {
        this.tableRef = tableRef;
        this.progress = progress;
    }

    TableReference getTableRef() {
        return tableRef;
    }

    boolean hasPreviousProgress() {
        return progress.isPresent();
    }

    long getStaleValuesDeletedPreviously() {
        return progress.map(SweepProgress::staleValuesDeleted).orElse(0L);
    }

    long getCellsExaminedPreviously() {
        return progress.map(SweepProgress::cellTsPairsExamined).orElse(0L);
    }

    Optional<Long> getPreviousMinimumSweptTimestamp() {
        return progress.map(SweepProgress::minimumSweptTimestamp);
    }

    byte[] getStartRow() {
        return progress.map(SweepProgress::startRow).orElse(PtBytes.EMPTY_BYTE_ARRAY);
    }
}
