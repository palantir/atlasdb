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

package com.palantir.atlasdb.transaction.service;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.api.ImmutableDependentState;
import com.palantir.atlasdb.transaction.api.ImmutableFullyCommittedState;
import com.palantir.atlasdb.transaction.api.ImmutablePrimaryTransactionLocator;
import com.palantir.atlasdb.transaction.api.ImmutableRolledBackState;
import com.palantir.atlasdb.transaction.api.RemoteTransactionServiceCache;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.DependentState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.FullyCommittedState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.PrimaryTransactionLocator;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.RolledBackState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.Visitor;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.jetbrains.annotations.Nullable;

/**
 * This is meant to be a view into the transaction timelines for users that don't need joint transactions. If the
 * current namespace is a secondary namespace, it is assumed that synchronisation of timestamps has already happened.
 *
 * TODO (jkong): I'm a bit worried about performance, there are a ton of layers stacking up everywhere. But this is
 * fine for hackweek.
 */
public class GenericUserFacingTransactionService implements TransactionService {
    private final CombinedTransactionService local;
    private final RemoteTransactionServiceCache factory;

    public GenericUserFacingTransactionService(
            CombinedTransactionService local, RemoteTransactionServiceCache factory) {
        this.local = local;
        this.factory = factory;
    }

    public static TransactionService create(
            Transactions3Service transactions3Service, RemoteTransactionServiceCache remoteTransactionServiceCache) {
        return new GenericUserFacingTransactionService(transactions3Service, remoteTransactionServiceCache);
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return Futures.immediateFuture(get(startTimestamp));
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return Futures.immediateFuture(get(startTimestamps));
    }

    @Nullable
    @Override
    public Long get(long startTimestamp) {
        Optional<TransactionCommittedState> dbState = local.getImmediateState(startTimestamp);
        if (dbState.isPresent()) {
            TransactionCommittedState state = dbState.get();
            return state.accept(new Visitor<>() {
                @Override
                public Long visitFullyCommitted(FullyCommittedState fullyCommittedState) {
                    return fullyCommittedState.commitTimestamp();
                }

                @Override
                public Long visitRolledBack(RolledBackState rolledBackState) {
                    return TransactionConstants.FAILED_COMMIT_TS;
                }

                @Override
                public Long visitDependent(DependentState dependentState) {
                    PrimaryTransactionLocator locator = dependentState.primaryLocator();
                    Long theirMaybeCommitState =
                            factory.getOrCreateForNamespace(locator.namespace()).get(locator.startTimestamp());
                    if (theirMaybeCommitState == null) {
                        // They're not committed yet, so we are not.
                        return null;
                    }
                    return theirMaybeCommitState == TransactionConstants.FAILED_COMMIT_TS
                            ? TransactionConstants.FAILED_COMMIT_TS
                            : dependentState.commitTimestamp();
                }
            });
        }

        // Transaction has not committed yet! Return null.
        return null;
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        // TODO (jkong): The number of times I've had to implement this hurts
        return KeyedStream.of(startTimestamps)
                .map(this::get)
                .filter(Objects::nonNull)
                .collectToMap();
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        TransactionCommittedState state;
        if (commitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
            state = ImmutableRolledBackState.builder().build();
        } else {
            state = ImmutableFullyCommittedState.builder()
                    .commitTimestamp(commitTimestamp)
                    .build();
        }
        local.putUnlessExists(startTimestamp, state);
    }

    @Override
    public void close() {
        // Responsibility for closing remotes is not on us
        local.close();
    }

    @Override
    public void putDependentInformation(
            long localStart, long localCommit, String foreignDependentName, long foreignDependentStart)
            throws KeyAlreadyExistsException {
        local.putUnlessExists(
                localStart,
                ImmutableDependentState.builder()
                        .commitTimestamp(localCommit)
                        .primaryLocator(ImmutablePrimaryTransactionLocator.builder()
                                .namespace(foreignDependentName)
                                .startTimestamp(foreignDependentStart)
                                .build())
                        .build());
    }

    @Override
    public void confirmDependentInformation(
            long localStart, long localCommit, String foreignCommitIdentity, long foreignStart)
            throws KeyAlreadyExistsException {
        local.checkAndSet(
                localStart,
                ImmutableDependentState.builder()
                        .commitTimestamp(localCommit)
                        .primaryLocator(ImmutablePrimaryTransactionLocator.builder()
                                .namespace(foreignCommitIdentity)
                                .startTimestamp(foreignStart)
                                .build())
                        .build(),
                ImmutableFullyCommittedState.builder()
                        .commitTimestamp(localCommit)
                        .build());
    }
}
