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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.transaction.api.StartedTransactionContext;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.immutables.value.Value;

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
        Map<String, LockToken> tokensToUnlock = Maps.newConcurrentMap();
        try {
            Map<String, Transaction> allTransactions = ImmutableMap.<String, Transaction>builder()
                    .putAll(KeyedStream.stream(dependentTransactions)
                            .map(StartedTransactionContext::startedTransaction)
                            .collectToMap())
                    .put(leadTransactionManagerIdentifier, context.startedTransaction())
                    .build();

            // give the user the stuff (task.execute something)
            JointTransaction jointTransaction = ImmutableJointTransaction.builder()
                    .constituentTransactions(allTransactions)
                    .build();
            T userOutput = task.execute(jointTransaction);

            // user's task is done, and transactions have buffered writes.
            // now to finish all the transactions!
            // first do the early stages

            // This is a view executor, so not too expensive
            Map<String, ExecutorService> namedExecutors = KeyedStream.stream(allTransactions)
                    .map((name, $) ->
                            PTExecutors.newSingleThreadExecutor(new NamedThreadFactory(name + "-runner", false)))
                    .collectToMap();
            CyclicBarrier barrier = new CyclicBarrier(allTransactions.size());
            Map<String, Future<ImmutablePhaseOneCommitOutput>> phaseOneCommitFutures = KeyedStream.stream(
                            namedExecutors)
                    .map((name, executor) -> executor.submit(() -> {
                        try {
                            Transaction responsibleTransaction = allTransactions.get(name);
                            boolean shouldOperate = !responsibleTransaction.runCommitPhaseOne();
                            uncheckedAwaitBarrier(barrier);
                            if (shouldOperate) {
                                responsibleTransaction.runCommitPhaseTwo();
                            }
                            uncheckedAwaitBarrier(barrier);
                            boolean hasWrites = false;
                            if (shouldOperate) {
                                hasWrites = !responsibleTransaction.runCommitPhaseThree();
                            }
                            uncheckedAwaitBarrier(barrier);
                            LockToken token = null;
                            if (shouldOperate && hasWrites) {
                                token = responsibleTransaction.runCommitPhaseFour();
                                tokensToUnlock.put(name, token);
                            }
                            uncheckedAwaitBarrier(barrier);
                            if (shouldOperate && hasWrites) {
                                responsibleTransaction.runCommitPhaseFive(
                                        knownTransactionManagers.get(name).getTransactionService(), token);
                            }
                            uncheckedAwaitBarrier(barrier);
                            if (!hasWrites) {
                                return ImmutablePhaseOneCommitOutput.builder()
                                        .shouldOperate(shouldOperate)
                                        .hasWrites(false)
                                        .build();
                            }
                            return ImmutablePhaseOneCommitOutput.builder()
                                    .shouldOperate(shouldOperate)
                                    .hasWrites(true)
                                    .lockToken(token)
                                    .build();
                        } finally {
                            barrier.reset();
                        }
                    }))
                    .collectToMap();

            Map<String, ImmutablePhaseOneCommitOutput> phaseOneCommits = KeyedStream.stream(phaseOneCommitFutures)
                    .map(Futures::getUnchecked)
                    .collectToMap();

            // At this point, all prep up to getting a commit timestamp is done. We now do this on the lead, and push
            // it to everyone else.
            Transaction leadTransaction = context.startedTransaction();
            PhaseOneCommitOutput leadPhaseOneCommitOutput = phaseOneCommits.get(leadTransactionManagerIdentifier);
            long globalCommitTimestamp = leadPhaseOneCommitOutput
                    .lockToken()
                    .map(leadTransaction::runCommitPhaseSix)
                    .orElseGet(() -> leadTransactionManager.getTimelockService().getFreshTimestamp());

            // Now do phase seven and eight, plus the first part of 9...
            // PUE into others with dependent state. If ALL successful, continue otherwise break.
            Map<String, Future<Object>> phaseTwoFutures = KeyedStream.stream(namedExecutors)
                    .map((name, executor) -> executor.submit(() -> {
                        try {
                            PhaseOneCommitOutput phaseOneCommitOutput = phaseOneCommits.get(name);
                            Transaction responsibleTransaction = allTransactions.get(name);

                            if (phaseOneCommitOutput.hasWrites()) {
                                responsibleTransaction.runCommitPhaseSevenDependently(globalCommitTimestamp);
                            }
                            uncheckedAwaitBarrier(barrier);
                            if (phaseOneCommitOutput.hasWrites()) {
                                LockToken commitLocksToken = phaseOneCommitOutput
                                        .lockToken()
                                        .orElseThrow(() -> new SafeIllegalStateException(
                                                "Not expecting lock token to be null if " + "we have writes!"));
                                responsibleTransaction.runCommitPhaseEight(commitLocksToken);
                            }
                            uncheckedAwaitBarrier(barrier);

                            // phase 9a
                            if (!name.equals(leadTransactionManagerIdentifier) && phaseOneCommitOutput.hasWrites()) {
                                responsibleTransaction.runCommitPhasePreNineDependently(
                                        leadTransactionManagerIdentifier,
                                        context.startedTransaction().getTimestamp(),
                                        globalCommitTimestamp);
                            }
                            uncheckedAwaitBarrier(barrier);

                            // phase 9b

                            if (name.equals(leadTransactionManagerIdentifier)) {
                                // IMPORTANT: Run this even if we didn't have writes in the lead transaction
                                if (phaseOneCommitOutput.hasWrites()) {
                                    LockToken commitLocksToken = phaseOneCommitOutput
                                            .lockToken()
                                            .orElseThrow(() -> new SafeIllegalStateException(
                                                    "Not expecting lock token to be null if " + "we have writes!"));
                                    responsibleTransaction.runCommitPhaseNine(commitLocksToken, globalCommitTimestamp);
                                } else {
                                    // TODO (jkong): Naughty
                                    responsibleTransaction.runCommitPhaseNine(null, globalCommitTimestamp);
                                }
                            }

                            uncheckedAwaitBarrier(barrier);

                            // phase 9c
                            // TODO (jkong): Technically the transaction is committed after phase 9 and a fail here
                            //  still implies successful commit. But ok.

                            if (!name.equals(leadTransactionManagerIdentifier) && phaseOneCommitOutput.hasWrites()) {
                                responsibleTransaction.runCommitPhasePostNineDependently(
                                        leadTransactionManagerIdentifier,
                                        context.startedTransaction().getTimestamp(),
                                        globalCommitTimestamp);
                            }

                            uncheckedAwaitBarrier(barrier);
                            return null;
                        } finally {
                            barrier.reset();
                        }
                    }))
                    .collectToMap();
            KeyedStream.stream(phaseTwoFutures).map(Futures::getUnchecked).collectToMap();

            return userOutput;
        } finally {
            List<Transaction> orderedTransactions = ImmutableList.<Transaction>builder()
                    .add(context.startedTransaction())
                    .addAll(dependentTransactions.values().stream()
                            .map(StartedTransactionContext::startedTransaction)
                            .collect(Collectors.toList()))
                    .build();
            for (Transaction transaction : orderedTransactions) {
                if (transaction.isUncommitted()) {
                    transaction.abort();
                }
            }

            releaseLocks(leadTransactionManager, context, dependentTransactions);
            tokensToUnlock.forEach((namespace, token) -> {
                knownTransactionManagers.get(namespace).getTimelockService().tryUnlock(ImmutableSet.of(token));
            });
        }
    }

    private void uncheckedAwaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
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
        leadTransactionManager
                .getTimelockService()
                .tryUnlock(
                        ImmutableSet.of(context.lockImmutableTimestampResponse().getLock()));
    }

    @Value.Immutable
    interface PhaseOneCommitOutput {
        boolean shouldOperate();

        boolean hasWrites();

        Optional<LockToken> lockToken();
    }
}
