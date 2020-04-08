/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.transaction.state;

import static com.palantir.logsafe.Preconditions.checkState;

import java.util.Iterator;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;

import org.inferred.freebuilder.FreeBuilder;

import com.palantir.atlasdb.v2.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.NewValue.TransactionValue;
import com.palantir.atlasdb.v2.api.ScanAttributes;
import com.palantir.atlasdb.v2.api.ScanFilter;
import com.palantir.atlasdb.v2.api.locks.NewLockDescriptor;
import com.palantir.atlasdb.v2.api.locks.NewLockToken;

@FreeBuilder
public abstract class TransactionState {
    public abstract Executor scheduler();
    public abstract long immutableTimestamp();
    abstract long startTimestamp();
    abstract OptionalLong commitTimestamp();
    public abstract Set<NewLockToken> heldLocks();
    public abstract TransactionReads reads();
    public abstract TransactionWrites writes();
    public abstract Builder toBuilder();

    public final boolean checkReadWriteConflicts(Table table) {
        throw new UnsupportedOperationException();
    }

    public static TransactionState newTransaction(
            Executor scheduler, long immutableTimestamp, long startTimestamp, NewLockToken immutableLockToken) {
        return new Builder()
                .scheduler(scheduler)
                .immutableTimestamp(immutableTimestamp)
                .addHeldLocks(immutableLockToken)
                .startTimestamp(startTimestamp)
                .reads(TransactionReads.EMPTY)
                .writes(TransactionWrites.EMPTY)
                .build();
    }

    public static class Builder extends TransactionState_Builder {
        @Override
        public Builder commitTimestamp(long commitTimestamp) {
            checkState(!super.commitTimestamp().isPresent(), "Cannot set commit timestamp when already set");
            return super.commitTimestamp(commitTimestamp);
        }
    }

    public Set<NewLockDescriptor> writeLockDescriptors() {
        throw new UnsupportedOperationException();
    }

    public TransactionState addWrite(Table table, TransactionValue value) {
        return toBuilder()
                .mutateWrites(writes -> writes.mutateWrites(table, tableWrites -> tableWrites.put(value)))
                .build();
    }

    public Iterator<TransactionValue> scan(Table table, ScanAttributes attributes, ScanFilter filter) {
        return null;
    }

}
