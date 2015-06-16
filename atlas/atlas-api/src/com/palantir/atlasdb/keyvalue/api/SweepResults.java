// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.api;

import java.util.Arrays;

import javax.annotation.Nullable;

import com.google.common.base.Optional;

public class SweepResults {
    private final @Nullable byte[] nextStartRow;
    private final long cellsExamined;
    private final long cellsSwept;

    public SweepResults(@Nullable byte[] nextStartRow, long cellsExamined, long cellsSwept) {
        this.nextStartRow = nextStartRow;
        this.cellsExamined = cellsExamined;
        this.cellsSwept = cellsSwept;
    }

    public Optional<byte[]> getNextStartRow() {
        return Optional.fromNullable(nextStartRow);
    }

    public long getCellsExamined() {
        return cellsExamined;
    }

    public long getCellsDeleted() {
        return cellsSwept;
    }

    @Override
    public String toString() {
        return "SweepResults [nextStartRow=" + Arrays.toString(nextStartRow) + ", cellsExamined="
                + cellsExamined + ", cellsSwept=" + cellsSwept + "]";
    }
}
