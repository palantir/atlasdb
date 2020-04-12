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

import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.inferred.freebuilder.FreeBuilder;

import com.google.common.collect.Streams;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewLockDescriptor;
import com.palantir.atlasdb.v2.api.api.NewLockToken;
import com.palantir.atlasdb.v2.api.util.Unreachable;

@FreeBuilder
public abstract class TransactionState {
    public abstract boolean debugging();

    public abstract Executor scheduler();
    public abstract ReadBehaviour readBehaviour();
    public abstract long immutableTimestamp();
    public abstract long startTimestamp();
    public abstract OptionalLong commitTimestamp();
    public abstract Set<NewLockToken> heldLocks();
    public abstract TransactionReads reads();
    public abstract TransactionWrites writes();
    public abstract Builder toBuilder();

    public final long readTimestamp() {
        switch (readBehaviour()) {
            case IN_TRANSACTION: return startTimestamp();
            case CHECKING_WRITE_WRITE_CONFLICTS: return Long.MAX_VALUE;
            case CHECKING_READ_WRITE_CONFLICTS: return commitTimestamp().getAsLong();
        }
        throw Unreachable.unreachable(readBehaviour());
    }

    public final boolean checkReadWriteConflicts(Table table) {
        return true;
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
        public Builder() {
            debugging(false);
            readBehaviour(ReadBehaviour.IN_TRANSACTION);
        }

        @Override
        public Builder commitTimestamp(long commitTimestamp) {
            checkState(!super.commitTimestamp().isPresent(), "Cannot set commit timestamp when already set");
            return super.commitTimestamp(commitTimestamp);
        }
    }

    public Set<NewLockDescriptor> writeLockDescriptors() {
        return Stream.concat(Stream.of(NewLockDescriptor.timestamp(startTimestamp())),
                Streams.stream(writes()).flatMap(tableWrites -> tableWrites.data().keySet().toJavaStream()
                        .map(cell -> NewLockDescriptor.cell(tableWrites.table(), cell))))
                .collect(Collectors.toSet());
    }
}
