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
import com.palantir.lock.watch.LockWatchReferences.EntireTable;
import com.palantir.lock.watch.LockWatchReferences.ExactCell;
import com.palantir.lock.watch.LockWatchReferences.ExactRow;
import com.palantir.lock.watch.LockWatchReferences.RowPrefix;
import com.palantir.lock.watch.LockWatchReferences.RowRange;
import java.util.Optional;

public final class LockWatchReferencesVisitor implements LockWatchReferences.Visitor<Optional<TableReference>> {
    public static final LockWatchReferencesVisitor INSTANCE = new LockWatchReferencesVisitor();

    @Override
    public Optional<TableReference> visit(EntireTable reference) {
        return Optional.of(TableReference.createFromFullyQualifiedName(reference.qualifiedTableRef()));
    }

    @Override
    public Optional<TableReference> visit(RowPrefix reference) {
        return Optional.empty();
    }

    @Override
    public Optional<TableReference> visit(RowRange reference) {
        return Optional.empty();
    }

    @Override
    public Optional<TableReference> visit(ExactRow reference) {
        return Optional.empty();
    }

    @Override
    public Optional<TableReference> visit(ExactCell reference) {
        return Optional.empty();
    }
}
