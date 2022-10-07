/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.knowledge.ImmutableTransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.knowledge.KnownAbortedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.service.AsyncTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class CommitTimestampLoaderTest {
    private static final TableReference TABLE_REF = TableReference.fromString("table");
    private final TimestampCache timestampCache = mock(TimestampCache.class);
    private final TransactionConfig transactionConfig = mock(TransactionConfig.class);
    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final TimelockService timelockService = mock(TimelockService.class);
    private final AsyncTransactionService transactionService = mock(AsyncTransactionService.class);

    private final KnownAbortedTransactions knownAbortedTransactions = mock(KnownAbortedTransactions.class);

    private final KnownConcludedTransactions knownConcludedTransactions = mock(KnownConcludedTransactions.class);

    private void setup(long startTs, long commitTs) {
        TransactionStatus commitStatus = TransactionStatuses.committed(commitTs);
        setup(startTs, commitStatus, false);
    }

    private void setup(long startTs, TransactionStatus commitStatus, boolean isAborted) {
        when(timestampCache.getCommitTimestampIfPresent(anyLong())).thenReturn(null);
        when(knownAbortedTransactions.isKnownAborted(anyLong())).thenReturn(isAborted);
        when(transactionService.getAsyncV2(startTs)).thenReturn(Futures.immediateFuture(commitStatus));
    }

    @Test
    public void readOnlyDoesNotThrowForUnsweptTTSCell() throws ExecutionException, InterruptedException {
        long transactionTs = 27l;
        long startTs = 5l;
        long commitTs = startTs + 1;

        setup(startTs, commitTs);

        // no immutableTs lock for read-only transaction
        CommitTimestampLoader commitTimestampLoader = getCommitTsLoader(Optional.empty(), transactionTs, commitTs - 1);

        assertCanGetCommitTs(startTs, commitTs, commitTimestampLoader);
    }

    @Test
    public void throwIfTTSBeyondReadOnlyForSweptTTSCell() {
        long transactionTs = 27l;
        long startTs = 5l;
        TransactionStatus commitStatus = TransactionStatuses.unknown();

        setup(startTs, commitStatus, true);

        // no immutableTs lock for read-only transaction
        CommitTimestampLoader commitTimestampLoader =
                getCommitTsLoader(Optional.empty(), transactionTs, transactionTs + 1);

        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> commitTimestampLoader
                        .getCommitTimestamps(TABLE_REF, ImmutableList.of(startTs), false, transactionService)
                        .get())
                .withRootCauseInstanceOf(SafeIllegalStateException.class)
                .withMessageContaining("Sweep has swept some entries with a commit TS after us");
    }

    @Test
    public void doNotThrowIfTTSBeyondReadOnlyTxnForNonTTSCell() throws ExecutionException, InterruptedException {
        long transactionTs = 27l;
        long startTs = 5l;
        long commitTs = startTs + 1;

        setup(startTs, commitTs);

        // no immutableTs lock for read-only transaction
        CommitTimestampLoader commitTimestampLoader =
                getCommitTsLoader(Optional.empty(), transactionTs, transactionTs + 1);

        assertCanGetCommitTs(startTs, commitTs, commitTimestampLoader);
    }

    @Test
    public void doNotThrowIfTTSBeyondReadWriteTxnForTTSCell() throws ExecutionException, InterruptedException {
        long transactionTs = 27l;
        long startTs = 5l;
        long commitTs = TransactionStatusUtils.getCommitTsForNonAbortedUnknownTransaction(startTs);

        setup(startTs, commitTs);

        LockToken lock = mock(LockToken.class);

        // no immutableTs lock for read-only transaction
        CommitTimestampLoader commitTimestampLoader =
                getCommitTsLoader(Optional.of(lock), transactionTs, transactionTs + 1);

        // the transaction will eventually throw at commit time. In this test we are only concerned with per read
        // validation.
        assertCanGetCommitTs(startTs, commitTs, commitTimestampLoader);
    }

    @Test
    public void doNotThrowIfTTSBeyondReadWriteTxnForNonTTSCell() throws ExecutionException, InterruptedException {
        long transactionTs = 27l;
        long startTs = 5l;
        long commitTs = startTs + 1;

        setup(startTs, commitTs);

        LockToken lock = mock(LockToken.class);

        // no immutableTs lock for read-only transaction
        CommitTimestampLoader commitTimestampLoader = getCommitTsLoader(Optional.of(lock), transactionTs, commitTs + 1);
        assertCanGetCommitTs(startTs, commitTs, commitTimestampLoader);
    }

    @Test
    public void doesNotCacheUnknownTransactions() throws ExecutionException, InterruptedException {
        long transactionTs = 27l;

        long startTsKnown = 5l;
        long commitTsKnown = startTsKnown + 1;

        long startTsUnknown = 7l;
        TransactionStatus commitUnknown = TransactionStatuses.unknown();

        CommitTimestampLoader commitTimestampLoader =
                getCommitTsLoader(Optional.empty(), transactionTs, transactionTs - 1);

        setup(startTsKnown, commitTsKnown);
        // the transaction will eventually throw at commit time. In this test we are only concerned with per read
        // validation.
        assertCanGetCommitTs(startTsKnown, commitTsKnown, commitTimestampLoader);
        verify(timestampCache).getCommitTimestampIfPresent(startTsKnown);
        verify(timestampCache).putAlreadyCommittedTransaction(startTsKnown, commitTsKnown);

        setup(startTsUnknown, commitUnknown, false);
        assertCanGetCommitTs(
                startTsUnknown,
                TransactionStatusUtils.getCommitTsForNonAbortedUnknownTransaction(startTsUnknown),
                commitTimestampLoader);
        verify(timestampCache).getCommitTimestampIfPresent(startTsUnknown);
        verifyNoMoreInteractions(timestampCache);
    }

    private void assertCanGetCommitTs(long startTs, long commitTs, CommitTimestampLoader commitTimestampLoader)
            throws InterruptedException, ExecutionException {
        Map<Long, Long> loadedCommitTs = commitTimestampLoader
                .getCommitTimestamps(TABLE_REF, ImmutableList.of(startTs), false, transactionService)
                .get();
        assertThat(loadedCommitTs).hasSize(1);
        assertThat(loadedCommitTs.get(startTs)).isEqualTo(commitTs);
    }

    private CommitTimestampLoader getCommitTsLoader(
            Optional<LockToken> lock, long transactionTs, long lastSeenCommitTs) {
        createKnowledgeComponents(lastSeenCommitTs);
        CommitTimestampLoader commitTimestampLoader = new CommitTimestampLoader(
                timestampCache,
                lock, // commitTsLoader does not care if the lock expires.
                () -> transactionTs,
                () -> transactionConfig,
                metricsManager,
                timelockService,
                1l,
                createKnowledgeComponents(lastSeenCommitTs));
        return commitTimestampLoader;
    }

    private TransactionKnowledgeComponents createKnowledgeComponents(long lastSeenCommitTs) {
        return ImmutableTransactionKnowledgeComponents.builder()
                .aborted(knownAbortedTransactions)
                .concluded(knownConcludedTransactions)
                .lastSeenCommitSupplier(() -> lastSeenCommitTs)
                .build();
    }
}
