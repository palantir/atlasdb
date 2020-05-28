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

package com.palantir.atlasdb.transaction.impl.watch;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Streams;
import com.palantir.atlasdb.keyvalue.api.watch.ImmutableStartTransactionsResponse;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchTransactionManager;
import com.palantir.atlasdb.keyvalue.api.watch.StartTransactionRequest;
import com.palantir.atlasdb.keyvalue.api.watch.StartTransactionsResponse;
import com.palantir.atlasdb.transaction.api.AutoDelegate_TransactionManager;
import com.palantir.atlasdb.transaction.api.OpenTransaction;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.ForwardingTransaction;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.TransactionsLockWatchEvents;

public class LockWatchTransactionManagerImpl implements AutoDelegate_TransactionManager, LockWatchTransactionManager {

    private final TransactionManager transactionManager;
    private final LockWatchEventCache lockWatchEventCache;

    public LockWatchTransactionManagerImpl(
            TransactionManager transactionManager,
            LockWatchEventCache lockWatchEventCache) {
        this.transactionManager = transactionManager;
        this.lockWatchEventCache = lockWatchEventCache;
    }

    @Override
    public TransactionManager delegate() {
        return transactionManager;
    }

    @Override
    public StartTransactionsResponse startTransactions(Optional<IdentifiedVersion> lastKnownVersion,
            List<StartTransactionRequest> requests) {
        List<CombinedCondition> amendedConditions = requests.stream().map(
                request -> new CombinedCondition(request.preCommitCondition(),
                        new WatchPreCommitCondition(lockWatchEventCache, request.watchCommitCondition())))
                .collect(Collectors.toList());
        List<OpenTransaction> responses = delegate().startTransactions(amendedConditions);
        List<OpenTransactionImpl> amendedTransactions = Streams.zip(
                responses.stream(),
                amendedConditions.stream(),
                (response, condition) -> {
                    condition.getSecondCondition().initialize(response);
                    return new OpenTransactionImpl(response, condition.getSecondCondition());
                })
                .collect(Collectors.toList());
        TransactionsLockWatchEvents eventsForTransactions = lockWatchEventCache.getEventsForTransactions(
                amendedTransactions.stream().map(Transaction::getTimestamp).collect(
                        Collectors.toSet()), lastKnownVersion);
        return ImmutableStartTransactionsResponse.builder()
                .addAllTransactions(amendedTransactions)
                .lockWatchEvents(eventsForTransactions)
                .build();
    }

    private final class OpenTransactionImpl extends ForwardingTransaction implements OpenTransaction {

        private final OpenTransaction delegate;
        private final PreCommitCondition condition;

        private OpenTransactionImpl(OpenTransaction delegate,
                PreCommitCondition condition) {
            this.delegate = delegate;
            this.condition = condition;
        }

        @Override
        public <T, E extends Exception> T finish(TransactionTask<T, E> task)
                throws E, TransactionFailedRetriableException {
            try {
                return delegate.finish(task);
            } finally {
                condition.cleanup();
            }
        }

        @Override
        public Transaction delegate() {
            return delegate;
        }
    }
}
