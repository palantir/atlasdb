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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.AtlasLockDescriptorUtils;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.lock.EteLockWatchResource;
import com.palantir.atlasdb.lock.GetLockWatchUpdateRequest;
import com.palantir.atlasdb.lock.SimpleEteLockWatchResource;
import com.palantir.atlasdb.lock.TransactionId;
import com.palantir.atlasdb.lock.WriteRequest;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.CommitUpdate.Visitor;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public final class LockWatchEteTest {
    private static final String SEED = "seed";
    private static final String ROW_1 = row(1);
    private static final String ROW_2 = row(2);
    private static final String ROW_3 = row(3);

    private final EteLockWatchResource lockWatcher = EteSetup.createClientToSingleNode(EteLockWatchResource.class);

    @Rule
    public final TestRule flakeRetryingRule = new FlakeRetryingRule();

    private TableReference tableReference;

    @Before
    public void before() {
        String tableName = UUID.randomUUID().toString().substring(0, 16).replace("-", "_");
        lockWatcher.setTable(tableName);
        this.tableReference = TableReference.create(SimpleEteLockWatchResource.NAMESPACE, tableName);
    }

    @Test
    public void commitUpdatesDoNotContainTheirOwnCommitLocks() {
        TransactionId firstTxn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(firstTxn, ROW_1));

        TransactionId secondTxn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(secondTxn, ROW_2));

        CommitUpdate firstUpdate = lockWatcher.endTransaction(firstTxn).get();
        CommitUpdate secondUpdate = lockWatcher.endTransaction(secondTxn).get();

        assertThat(extractDescriptorsFromUpdate(firstUpdate)).isEmpty();
        assertThat(extractDescriptorsFromUpdate(secondUpdate))
                .containsExactlyInAnyOrderElementsOf(getDescriptors(ROW_1));
    }

    @Test
    @ShouldRetry
    public void multipleTransactionVersionsReturnsSnapshotAndOnlyRelevantRecentEvents() {
        LockWatchVersion baseVersion = seedCacheAndGetVersion();

        writeValues(ROW_1, ROW_2);
        TransactionId secondTxn = lockWatcher.startTransaction();

        writeValues(row(3));
        TransactionId fourthTxn = lockWatcher.startTransaction();

        TransactionsLockWatchUpdate update = lockWatcher.getUpdate(GetLockWatchUpdateRequest.of(
                ImmutableSet.of(secondTxn.startTs(), fourthTxn.startTs()), Optional.empty()));

        assertThat(update.clearCache()).isTrue();
        assertThat(update.startTsToSequence().get(secondTxn.startTs()).version())
                .isEqualTo(baseVersion.version() + 2);
        assertThat(update.startTsToSequence().get(fourthTxn.startTs()).version())
                .isEqualTo(baseVersion.version() + 4);
        assertThat(lockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(row(3)));
        assertThat(unlockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(row(3)));
        assertThat(watchDescriptors(update.events())).isEmpty();

        lockWatcher.endTransaction(secondTxn);
        lockWatcher.endTransaction(fourthTxn);
    }

    @Test
    public void upToDateVersionReturnsOnlyNecessaryEvents() {
        LockWatchVersion baseVersion = seedCacheAndGetVersion();

        writeValues(ROW_1);
        TransactionId firstTxn = lockWatcher.startTransaction();
        writeValues(ROW_2);
        LockWatchVersion currentVersion = getCurrentVersion();
        writeValues(ROW_3);
        TransactionId secondTxn = lockWatcher.startTransaction();

        TransactionsLockWatchUpdate update = lockWatcher.getUpdate(GetLockWatchUpdateRequest.of(
                ImmutableSet.of(firstTxn.startTs(), secondTxn.startTs()), Optional.of(currentVersion)));

        assertThat(update.clearCache()).isFalse();
        assertThat(update.startTsToSequence().get(firstTxn.startTs()).version()).isEqualTo(baseVersion.version() + 2);
        assertThat(update.startTsToSequence().get(secondTxn.startTs()).version())
                .isEqualTo(currentVersion.version() + 2);
        assertThat(lockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(row(3)));
        assertThat(unlockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(row(3)));
        assertThat(watchDescriptors(update.events())).isEmpty();
    }

    private void writeValues(String... rows) {
        TransactionId txn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(txn, rows));
        lockWatcher.endTransaction(txn);
    }

    private LockWatchVersion seedCacheAndGetVersion() {
        TransactionId txn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(txn, SEED));
        lockWatcher.endTransaction(txn);

        return getCurrentVersion();
    }

    private Set<LockDescriptor> lockedDescriptors(Collection<LockWatchEvent> events) {
        return getDescriptorsFromLockWatchEvent(events, LockEventVisitor.INSTANCE);
    }

    private Set<LockDescriptor> unlockedDescriptors(Collection<LockWatchEvent> events) {
        return getDescriptorsFromLockWatchEvent(events, UnlockEventVisitor.INSTANCE);
    }

    private Set<LockDescriptor> watchDescriptors(Collection<LockWatchEvent> events) {
        return getDescriptorsFromLockWatchEvent(events, WatchEventVisitor.INSTANCE);
    }

    private Set<LockDescriptor> getDescriptorsFromLockWatchEvent(
            Collection<LockWatchEvent> events, LockWatchEvent.Visitor<Set<LockDescriptor>> visitor) {
        return filterDescriptors(events.stream()
                .map(event -> event.accept(visitor))
                .flatMap(Set::stream)
                .collect(Collectors.toSet()));
    }

    private LockWatchVersion getCurrentVersion() {
        TransactionId emptyTxn = lockWatcher.startTransaction();
        TransactionsLockWatchUpdate update = lockWatcher.getUpdate(
                GetLockWatchUpdateRequest.of(ImmutableSet.of(emptyTxn.startTs()), Optional.empty()));
        LockWatchVersion version = update.startTsToSequence().get(emptyTxn.startTs());
        lockWatcher.endTransaction(emptyTxn);
        return version;
    }

    private Set<LockDescriptor> filterDescriptors(Set<LockDescriptor> descriptors) {
        return descriptors.stream()
                .filter(desc -> AtlasLockDescriptorUtils.tryParseTableRef(desc)
                        .get()
                        .tableRef()
                        .equals(tableReference))
                .collect(Collectors.toSet());
    }

    private Set<LockDescriptor> getDescriptors(String... rows) {
        return Stream.of(rows)
                .map(row -> AtlasRowLockDescriptor.of(this.tableReference.getQualifiedName(), PtBytes.toBytes(row)))
                .collect(Collectors.toSet());
    }

    private static String row(int index) {
        return "row" + index;
    }

    private Set<LockDescriptor> extractDescriptorsFromUpdate(CommitUpdate commitUpdate) {
        return filterDescriptors(commitUpdate.accept(new Visitor<Set<LockDescriptor>>() {
            @Override
            public Set<LockDescriptor> invalidateAll() {
                return ImmutableSet.of();
            }

            @Override
            public Set<LockDescriptor> invalidateSome(Set<LockDescriptor> invalidatedLocks) {
                return invalidatedLocks;
            }
        }));
    }

    private static final class LockEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        static final LockEventVisitor INSTANCE = new LockEventVisitor();

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            return lockEvent.lockDescriptors();
        }

        @Override
        public Set<LockDescriptor> visit(UnlockEvent unlockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return ImmutableSet.of();
        }
    }

    private static final class UnlockEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        static final UnlockEventVisitor INSTANCE = new UnlockEventVisitor();

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(UnlockEvent unlockEvent) {
            return unlockEvent.lockDescriptors();
        }

        @Override
        public Set<LockDescriptor> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return ImmutableSet.of();
        }
    }

    private static final class WatchEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        static final WatchEventVisitor INSTANCE = new WatchEventVisitor();

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(UnlockEvent unlockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return lockWatchCreatedEvent.lockDescriptors();
        }
    }
}
