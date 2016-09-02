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
package com.palantir.atlasdb.keyvalue.api;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.palantir.atlasdb.encoding.PtBytes;

public class SweepResults {
    private final @Nullable byte[] nextStartRow;
    private final long cellsExamined;
    private final long cellsSwept;
    private final long sweptTimestamp;

    public static SweepResults createEmptySweepResult(long sweptTimestamp) {
        return new SweepResults(null, 0, 0, sweptTimestamp);
    }

    public SweepResults(@Nullable byte[] nextStartRow, long cellsExamined, long cellsSwept, long sweptTimestamp) {
        this.nextStartRow = nextStartRow;
        this.cellsExamined = cellsExamined;
        this.cellsSwept = cellsSwept;
        this.sweptTimestamp = sweptTimestamp;
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

    public long getSweptTimestamp() {
        return sweptTimestamp;
    }

    @Override
    public String toString() {
        return "SweepResults [nextStartRow=" + PtBytes.encodeHexString(nextStartRow)
                + ", cellsExamined=" + cellsExamined
                + ", cellsSwept=" + cellsSwept
                + ", sweptTimestamp=" + sweptTimestamp + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SweepResults that = (SweepResults) obj;
        return cellsExamined == that.cellsExamined
                && cellsSwept == that.cellsSwept
                && sweptTimestamp == that.sweptTimestamp
                && Arrays.equals(nextStartRow, that.nextStartRow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(nextStartRow), cellsExamined, cellsSwept, sweptTimestamp);
    }
}
