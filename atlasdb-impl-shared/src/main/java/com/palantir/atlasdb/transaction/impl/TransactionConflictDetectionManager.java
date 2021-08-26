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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

public final class TransactionConflictDetectionManager {

    private final ConflictDetectionManager delegate;
    private final Map<TableReference, Optional<ConflictHandler>> conflictHandlers = new ConcurrentHashMap<>();

    public TransactionConflictDetectionManager(ConflictDetectionManager delegate) {
        this.delegate = delegate;
    }

    public void disableReadWriteConflict(TableReference tableRef) {
        conflictHandlers.compute(tableRef, (tableReference, nullableCurrentValue) -> {
            Optional<ConflictHandler> conflictHandler = delegate.get(tableReference);
            Preconditions.checkNotNull(conflictHandler.isPresent(), "Conflict handler cannot be null when overwriting");
            Optional<ConflictHandler> newValue =
                    Optional.of(getDisabledReadWriteConflictHandler(conflictHandler.get()));

            Preconditions.checkState(
                    nullableCurrentValue == null || nullableCurrentValue.equals(newValue),
                    "Cannot overwrite conflict behaviour after the table has already been used");
            return newValue;
        });
    }

    private ConflictHandler getDisabledReadWriteConflictHandler(ConflictHandler conflictHandler) {
        if (!conflictHandler.checkReadWriteConflicts()) {
            return conflictHandler;
        }
        switch (conflictHandler) {
            case SERIALIZABLE:
                return ConflictHandler.RETRY_ON_WRITE_WRITE;
            case SERIALIZABLE_CELL:
                return ConflictHandler.RETRY_ON_WRITE_WRITE_CELL;
            case SERIALIZABLE_INDEX:
            case SERIALIZABLE_LOCK_LEVEL_MIGRATION:
                throw new UnsupportedOperationException();
            default:
                throw new SafeIllegalStateException(
                        "Unknown conflict handling strategy", SafeArg.of("conflictHandler", conflictHandler));
        }
    }

    @Nullable
    public ConflictHandler get(TableReference tableReference) {
        return conflictHandlers.computeIfAbsent(tableReference, delegate::get).orElse(null);
    }
}
