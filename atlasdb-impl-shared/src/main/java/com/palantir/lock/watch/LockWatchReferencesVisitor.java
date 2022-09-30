/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.WatchedCellRanges;
import com.palantir.atlasdb.transaction.api.RowReference;
import com.palantir.lock.watch.LockWatchReferences.EntireTable;
import com.palantir.lock.watch.LockWatchReferences.ExactCell;
import com.palantir.lock.watch.LockWatchReferences.ExactRow;
import com.palantir.lock.watch.LockWatchReferences.RowPrefix;
import com.palantir.lock.watch.LockWatchReferences.RowRange;

public final class LockWatchReferencesVisitor
        implements LockWatchReferences.Visitor<WatchedCellRanges.WatchedCellRange> {
    public static final LockWatchReferencesVisitor INSTANCE = new LockWatchReferencesVisitor();

    @Override
    public WatchedCellRanges.WatchedCellRange visit(EntireTable reference) {
        return WatchedCellRanges.WatchedTableReference.of(
                TableReference.createFromFullyQualifiedName(reference.qualifiedTableRef()));
    }

    @Override
    public WatchedCellRanges.WatchedCellRange visit(RowPrefix reference) {
        throw new UnsupportedOperationException("Row prefix watches are not yet supported");
    }

    @Override
    public WatchedCellRanges.WatchedCellRange visit(RowRange reference) {
        throw new UnsupportedOperationException("Row range watches are not yet supported");
    }

    @Override
    public WatchedCellRanges.WatchedCellRange visit(ExactRow reference) {
        return WatchedCellRanges.WatchedRowReference.of(RowReference.of(
                TableReference.createFromFullyQualifiedName(reference.qualifiedTableRef()), reference.row()));
    }

    @Override
    public WatchedCellRanges.WatchedCellRange visit(ExactCell reference) {
        throw new UnsupportedOperationException("Exact cell watches are not yet supported");
    }
}
