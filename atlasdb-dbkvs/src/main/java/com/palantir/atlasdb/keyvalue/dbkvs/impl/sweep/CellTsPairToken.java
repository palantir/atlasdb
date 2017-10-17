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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep;

import javax.annotation.Nullable;

import com.palantir.atlasdb.encoding.PtBytes;

public final class CellTsPairToken {
    public final byte[] startRowInclusive;
    public final byte[] startColInclusive;
    @Nullable
    public final Long startTsInclusive;
    public final boolean reachedEnd;

    private CellTsPairToken(byte[] startRowInclusive,
            byte[] startColInclusive,
            Long startTsInclusive,
            boolean reachedEnd) {
        this.startRowInclusive = startRowInclusive;
        this.startColInclusive = startColInclusive;
        this.startTsInclusive = startTsInclusive;
        this.reachedEnd = reachedEnd;
    }

    public static CellTsPairToken startRow(byte[] startRowInclusive) {
        return new CellTsPairToken(startRowInclusive, PtBytes.EMPTY_BYTE_ARRAY, null, false);
    }

    public static CellTsPairToken continueRow(CellTsPairInfo lastResult) {
        return new CellTsPairToken(lastResult.rowName, lastResult.colName, lastResult.ts + 1, false);
    }

    public static CellTsPairToken end() {
        return new CellTsPairToken(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, null, true);
    }
}
