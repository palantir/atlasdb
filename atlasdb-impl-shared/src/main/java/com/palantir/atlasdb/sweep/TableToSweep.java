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

import java.util.OptionalLong;

import javax.annotation.Nullable;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.progress.SweepProgress;

public final class TableToSweep {
    private final TableReference tableRef;
    @Nullable
    private final SweepProgress progress;

    TableToSweep(TableReference tableRef, SweepProgress progress) {
        this.tableRef = tableRef;
        this.progress = progress;
    }

    TableReference getTableRef() {
        return tableRef;
    }

    boolean hasPreviousProgress() {
        return progress != null;
    }

    long getStaleValuesDeletedPreviously() {
        return progress == null ? 0L : progress.staleValuesDeleted();
    }

    long getCellsExaminedPreviously() {
        return progress == null ? 0L : progress.cellTsPairsExamined();
    }

    OptionalLong getPreviousMinimumSweptTimestamp() {
        return progress == null ? OptionalLong.empty() : OptionalLong.of(progress.minimumSweptTimestamp());
    }

    byte[] getStartRow() {
        return progress == null ? PtBytes.EMPTY_BYTE_ARRAY : progress.startRow();
    }
}
