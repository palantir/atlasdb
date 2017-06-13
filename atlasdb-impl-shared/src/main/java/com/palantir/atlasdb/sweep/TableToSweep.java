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
