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

package com.palantir.atlasdb.transaction.joint;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.StartedTransactionContext;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import java.util.Map;

public class SimpleJointTransactionManager implements JointTransactionManager {
    private final Map<String, TransactionManager> knownTransactionManagers;
    private final String leadTransactionManagerIdentifier;

    public SimpleJointTransactionManager(
            Map<String, TransactionManager> knownTransactionManagers, String leadTransactionManagerIdentifier) {
        this.knownTransactionManagers = knownTransactionManagers;
        this.leadTransactionManagerIdentifier = leadTransactionManagerIdentifier;
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(JointTransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException {
        // start transactions, create a joint transaction
        TransactionManager leadTransactionManager = knownTransactionManagers.get(leadTransactionManagerIdentifier);
        StartedTransactionContext context = leadTransactionManager.startTransaction();
        Map<String, StartedTransactionContext> dependentTransactions = KeyedStream.stream(knownTransactionManagers)
                .filterKeys(identifier -> !identifier.equals(leadTransactionManagerIdentifier))
                .map(secondaryTxMgr -> secondaryTxMgr.createTransactionWithDependentContext(
                        context.startedTransaction().getTimestamp(), context.lockImmutableTimestampResponse()))
                .collectToMap();
        try {
            Map<String, Transaction> allTransactions = ImmutableMap.<String, Transaction>builder()
                    .putAll(dependentTransactions)
                    .put(leadTransactionManagerIdentifier, context.startedTransaction())
                    .build();

            // give the user the stuff (task.execute something)
            JointTransaction jointTransaction = ImmutableJointTransaction.builder()
                    .constituentTransactions(allTransactions)
                    .build();
            task.execute(jointTransaction);

            // user's task is done, and transactions have buffered writes.
            // now to finish all the transactions:
            // run the 1st part of the protocol
            // then
            // (1) PUE into others with dependent state. If ALL successful
            // (2) PUE myself with done
            // (3) CAS others (update?)
            return null;
        } finally {
            releaseLocks(leadTransactionManager, context, dependentTransactions);
        }
    }

    private void releaseLocks(
            TransactionManager leadTransactionManager,
            StartedTransactionContext context,
            Map<String, StartedTransactionContext> dependentTransactions) {
        Map<String, LockToken> tokensToUnlock = KeyedStream.stream(dependentTransactions)
                .map(StartedTransactionContext::lockImmutableTimestampResponse)
                .map(LockImmutableTimestampResponse::getLock)
                .collectToMap();
        tokensToUnlock.forEach((namespace, token) -> {
            knownTransactionManagers.get(namespace).getTimelockService().tryUnlock(ImmutableSet.of(token));
        });
        leadTransactionManager.getTimelockService().tryUnlock(ImmutableSet.of(context.lockImmutableTimestampResponse()
                .getLock()));
    }
}
