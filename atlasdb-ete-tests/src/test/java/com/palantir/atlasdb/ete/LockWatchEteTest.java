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
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.CommitUpdate.Visitor;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockWatchEteTest {
    private static final Logger log = LoggerFactory.getLogger(LockWatchEteTest.class);

    private static final String ROW_1 = row(1);
    private static final String ROW_2 = row(2);

    private final EteLockWatchResource lockWatcher = EteSetup.createClientToSingleNode(EteLockWatchResource.class);

    private TableReference tableReference;

    @Before
    public void before() {
        createTable();
    }

    @Test
    public void commitUpdatesDoNotContainTheirOwnCommitLocks() {
        TransactionId firstTxn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(firstTxn, ROW_1));

        TransactionId secondTxn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(secondTxn, ROW_2));

        CommitUpdate firstUpdate = lockWatcher.endTransaction(firstTxn).get();
        CommitUpdate secondUpdate = lockWatcher.endTransaction(secondTxn).get();

        assertThat(extractDescriptors(firstUpdate)).isEmpty();
        assertThat(extractDescriptors(secondUpdate)).containsExactlyInAnyOrder(getDescriptor(ROW_1));
    }

    @Test
    public void bleh() {
        LockWatchVersion baseVersion = seedCacheAndGetVersion();

        TransactionId firstTxn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(firstTxn, ROW_1, ROW_2));
        LockWatchVersion firstVersion = lockWatcher.getVersion(firstTxn);
        lockWatcher.endTransaction(firstTxn);

        TransactionId secondTxn = lockWatcher.startTransaction();
        TransactionId thirdTxn = lockWatcher.startTransaction();
        TransactionId fourthTxn = lockWatcher.startTransaction();

        lockWatcher.write(WriteRequest.of(thirdTxn, row(3)));
        LockWatchVersion thirdVersion = lockWatcher.getVersion(thirdTxn);
        lockWatcher.endTransaction(thirdTxn);

        TransactionsLockWatchUpdate update = lockWatcher.getUpdate(GetLockWatchUpdateRequest.of(
                ImmutableSet.of(secondTxn.startTs(), fourthTxn.startTs()), Optional.empty()));

        assertThat(update.clearCache()).isTrue();
        assertThat(update.startTsToSequence().get(secondTxn.startTs()).version())
                .isEqualTo(baseVersion.version() + 4);
        assertThat(update.startTsToSequence().get(fourthTxn.startTs()).version())
                .isEqualTo(baseVersion.version() + 6);
    }

    private LockWatchVersion seedCacheAndGetVersion() {
        TransactionId txn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(txn, "seed"));
        lockWatcher.endTransaction(txn);

        TransactionId emptyTxn = lockWatcher.startTransaction();
        LockWatchVersion version = lockWatcher.getVersion(emptyTxn);
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

    private static Set<LockDescriptor> extractDescriptors(CommitUpdate commitUpdate) {
        return commitUpdate.accept(new Visitor<Set<LockDescriptor>>() {
            @Override
            public Set<LockDescriptor> invalidateAll() {
                return ImmutableSet.of();
            }

            @Override
            public Set<LockDescriptor> invalidateSome(Set<LockDescriptor> invalidatedLocks) {
                return invalidatedLocks;
            }
        });
    }

    private static String row(int index) {
        return "row" + index;
    }

    private void createTable() {
        String tableName = UUID.randomUUID().toString().substring(0, 16);
        lockWatcher.setTable(tableName);
        this.tableReference = TableReference.create(SimpleEteLockWatchResource.NAMESPACE, tableName);
    }

    private LockDescriptor getDescriptor(String row) {
        return AtlasRowLockDescriptor.of(this.tableReference.getQualifiedName(), PtBytes.toBytes(row));
    }
}
