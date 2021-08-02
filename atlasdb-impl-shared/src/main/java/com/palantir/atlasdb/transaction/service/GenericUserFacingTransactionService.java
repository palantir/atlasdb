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
import com.palantir.atlasdb.transaction.api.ImmutableFullyCommittedState;
import com.palantir.atlasdb.transaction.api.ImmutableRolledBackState;
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
 */
public class GenericUserFacingTransactionService implements TransactionService {
    private final Map<String, CombinedTransactionService> identifiedCombinedTransactionServices;
    private final String defaultNamespace;

    public GenericUserFacingTransactionService(
            Map<String, CombinedTransactionService> identifiedCombinedTransactionServices,
            String defaultNamespace) {
        this.identifiedCombinedTransactionServices = identifiedCombinedTransactionServices;
        this.defaultNamespace = defaultNamespace;
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
        Optional<TransactionCommittedState> dbState =
                identifiedCombinedTransactionServices.get(defaultNamespace).getImmediateState(startTimestamp);
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
                    Optional<TransactionCommittedState> theirMaybeCommitState =
                            identifiedCombinedTransactionServices.get(locator.namespace())
                                    .getImmediateState(locator.startTimestamp());
                    if (theirMaybeCommitState.isEmpty()) {
                        // They're not committed yet, so we are not.
                        return null;
                    }
                    TransactionCommittedState theirCommittedState = theirMaybeCommitState.get();
                    return theirCommittedState.accept(this) == TransactionConstants.FAILED_COMMIT_TS ?
                            TransactionConstants.FAILED_COMMIT_TS : locator.startTimestamp();
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
            state = ImmutableFullyCommittedState.builder().commitTimestamp(commitTimestamp).build();
        }
        identifiedCombinedTransactionServices.get(defaultNamespace).putUnlessExists(startTimestamp, state);
    }

    @Override
    public void close() {
        identifiedCombinedTransactionServices.values().forEach(CombinedTransactionService::close);
    }
}
