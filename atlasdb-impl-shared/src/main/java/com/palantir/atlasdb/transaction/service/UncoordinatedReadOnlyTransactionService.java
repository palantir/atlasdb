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
import com.palantir.common.streams.KeyedStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.jetbrains.annotations.Nullable;

public class UncoordinatedReadOnlyTransactionService implements TransactionService {
    private final List<TransactionService> candidates;

    public UncoordinatedReadOnlyTransactionService(List<TransactionService> candidates) {
        this.candidates = candidates;
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
        return candidates.stream()
                .map(candidate -> Optional.ofNullable(candidate.get(startTimestamp)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElse(null);
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
        throw new UnsupportedOperationException("Cannot write");
    }

    @Override
    public void close() {
        candidates.forEach(TransactionService::close);
    }

    @Override
    public void putDependentInformation(
            long localStart, long localCommit, String foreignDependentName, long foreignDependentStart)
            throws KeyAlreadyExistsException {
        throw new UnsupportedOperationException("Cannot write");
    }

    @Override
    public void confirmDependentInformation(
            long localStart, long localCommit, String foreignCommitIdentity, long foreignStart)
            throws KeyAlreadyExistsException {
        throw new UnsupportedOperationException("Cannot write");
    }
}
