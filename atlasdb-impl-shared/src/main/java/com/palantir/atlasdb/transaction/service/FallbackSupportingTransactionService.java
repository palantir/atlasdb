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
import com.palantir.atlasdb.transaction.api.TransactionCommittedState;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class FallbackSupportingTransactionService implements CombinedTransactionService {
    private final CombinedTransactionService primary;
    private final CombinedTransactionService secondary;

    public FallbackSupportingTransactionService(
            CombinedTransactionService primary,
            CombinedTransactionService secondary) {
        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public Optional<TransactionCommittedState> getImmediateState(long startTimestamp) {
        return Stream.of(primary, secondary)
                .flatMap(service -> service.getImmediateState(startTimestamp).stream())
                .findFirst();
    }

    @Override
    public Map<Long, TransactionCommittedState> get(Iterable<Long> startTimestamps) {
        // TODO (jkong): Be more efficient, this is bad
        return KeyedStream.of(startTimestamps)
                .map(this::getImmediateState)
                .flatMap(Optional::stream)
                .collectToMap();
    }

    @Override
    public void putUnlessExists(long startTimestamp, TransactionCommittedState state) throws KeyAlreadyExistsException {
        primary.putUnlessExists(startTimestamp, state);
    }

    @Override
    public void checkAndSet(
            long startTimestamp, TransactionCommittedState expected, TransactionCommittedState newProposal)
            throws KeyAlreadyExistsException {
        primary.checkAndSet(startTimestamp, expected, newProposal);
    }

    @Override
    public void close() {
        primary.close();
        secondary.close();
    }
}
