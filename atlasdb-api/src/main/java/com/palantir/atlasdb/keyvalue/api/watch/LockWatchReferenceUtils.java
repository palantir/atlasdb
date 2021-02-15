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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.LockWatchReferences;

public final class LockWatchReferenceUtils {
    private LockWatchReferenceUtils() {
        // utility
    }

    public static LockWatchReferences.LockWatchReference entireTable(TableReference tableRef) {
        return LockWatchReferences.entireTable(tableRef.getQualifiedName());
    }

    public static LockWatchReferences.LockWatchReference rowPrefix(TableReference tableRef, byte[] rowPrefix) {
        return LockWatchReferences.rowPrefix(tableRef.getQualifiedName(), rowPrefix);
    }

    public static LockWatchReferences.LockWatchReference rowRange(
            TableReference tableRef, byte[] startInclusive, byte[] endExclusive) {
        return LockWatchReferences.rowRange(tableRef.getQualifiedName(), startInclusive, endExclusive);
    }

    public static LockWatchReferences.LockWatchReference exactRow(TableReference tableRef, byte[] row) {
        return LockWatchReferences.exactRow(tableRef.getQualifiedName(), row);
    }

    public static LockWatchReferences.LockWatchReference exactCell(TableReference tableRef, Cell cell) {
        return LockWatchReferences.exactCell(tableRef.getQualifiedName(), cell.getRowName(), cell.getColumnName());
    }
}
