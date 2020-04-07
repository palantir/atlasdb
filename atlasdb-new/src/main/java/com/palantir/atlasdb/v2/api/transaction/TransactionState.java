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

package com.palantir.atlasdb.v2.api.transaction;

import static com.palantir.logsafe.Preconditions.checkState;

import java.util.Iterator;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;

import com.palantir.atlasdb.v2.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.NewValue.TransactionValue;
import com.palantir.atlasdb.v2.api.ScanAttributes;
import com.palantir.atlasdb.v2.api.ScanFilter;
import com.palantir.atlasdb.v2.api.locks.NewLockDescriptor;
import com.palantir.atlasdb.v2.api.locks.NewLockToken;

import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.Map;
import io.vavr.collection.SortedMap;
import io.vavr.collection.TreeMap;

public final class TransactionState {
    private final Executor scheduler;
    private final long timestamp;
    private final OptionalLong commitTimestamp;
    private final HashSet<NewLockToken> heldLocks;
    private final Map<Table, TableWrites> writes;

    public TransactionState(Executor scheduler, long timestamp) {
        this(scheduler, timestamp, OptionalLong.empty(), HashSet.empty(), HashMap.empty());
    }

    public TransactionState(Executor scheduler, long timestamp, OptionalLong commitTimestamp,
            HashSet<NewLockToken> heldLocks,
            Map<Table, TableWrites> writes) {
        this.timestamp = timestamp;
        this.commitTimestamp = commitTimestamp;
        this.heldLocks = heldLocks;
        this.writes = writes;
        this.scheduler = scheduler;
    }

    private TransactionState withWrites(Map<Table, TableWrites> writes) {
        return new TransactionState(scheduler, timestamp, commitTimestamp, heldLocks, writes);
    }

    public TransactionState withCommitTimestamp(long commitTimestamp) {
        checkState(!this.commitTimestamp.isPresent());
        return new TransactionState(scheduler, timestamp, OptionalLong.of(commitTimestamp), heldLocks, writes);
    }

    public Executor getScheduler() {
        return scheduler;
    }

    public boolean hasWrites() {
        return !writes.find(entry -> !entry._2.isEmpty()).isEmpty();
    }

    public Set<NewLockToken> heldLocks() {
        return heldLocks.toJavaSet();
    }

    public TransactionState addHeldLock(NewLockToken token) {
        return new TransactionState(scheduler, timestamp, commitTimestamp, heldLocks.add(token), writes);
    }

    public Set<NewLockDescriptor> writeLockDescriptors() {
        throw new UnsupportedOperationException();
    }

    public TransactionState addWrite(Table table, TransactionValue value) {
        TableWrites currentWrites = writes.getOrElse(table, TableWrites.EMPTY);
        TableWrites newWrites = currentWrites.put(value);
        return withWrites(writes.put(table, newWrites));
    }

    public Iterator<TransactionValue> scan(Table table, ScanAttributes attributes, ScanFilter filter) {
        return null;
    }

    private static final class TableWrites {
        private static final TableWrites EMPTY = new TableWrites(TreeMap.empty());

        private final SortedMap<Cell, TransactionValue> writes;

        private TableWrites(SortedMap<Cell, TransactionValue> writes) {
            this.writes = writes;
        }

        private TableWrites put(TransactionValue value) {
            return new TableWrites(writes.put(value.cell(), value));
        }

        private boolean isEmpty() {
            return writes.isEmpty();
        }
    }
}
