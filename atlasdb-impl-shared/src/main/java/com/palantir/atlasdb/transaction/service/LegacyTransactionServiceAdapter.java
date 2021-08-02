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

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.api.ImmutableFullyCommittedState;
import com.palantir.atlasdb.transaction.api.ImmutableRolledBackState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.DependentState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.FullyCommittedState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.RolledBackState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.Visitor;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Map;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;

public class LegacyTransactionServiceAdapter implements CombinedTransactionService {
    private final TransactionService legacyService;

    public LegacyTransactionServiceAdapter(TransactionService legacyService) {
        this.legacyService = legacyService;
    }

    @Override
    public Optional<TransactionCommittedState> getImmediateState(long startTimestamp) {
        Long timestamp = legacyService.get(startTimestamp);
        return Optional.ofNullable(timestamp)
                .map(this::translateCommittedTimestamp);
    }

    @Override
    public Map<Long, TransactionCommittedState> get(Iterable<Long> startTimestamps) {
        return KeyedStream.stream(legacyService.get(startTimestamps))
                .map(this::translateCommittedTimestamp)
                .collectToMap();

    }

    @Override
    public void putUnlessExists(long startTimestamp, TransactionCommittedState state) throws KeyAlreadyExistsException {
        legacyService.putUnlessExists(startTimestamp, state.accept(new Visitor<>() {
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
                throw new SafeRuntimeException("naughty! shouldn't be putting dependent states in the old txn table");
            }
        }));
    }

    @Override
    public void checkAndSet(
            long startTimestamp, TransactionCommittedState expected, TransactionCommittedState newProposal)
            throws KeyAlreadyExistsException {
        throw new SafeRuntimeException("naughty! can't check and set in the old txn table");
    }

    @Override
    public void close() {
        legacyService.close();
    }

    @NotNull
    private TransactionCommittedState translateCommittedTimestamp(long present) {
        if (present == TransactionConstants.FAILED_COMMIT_TS) {
            return ImmutableRolledBackState.builder().build();
        }
        return ImmutableFullyCommittedState.builder().commitTimestamp(present).build();
    }
}
