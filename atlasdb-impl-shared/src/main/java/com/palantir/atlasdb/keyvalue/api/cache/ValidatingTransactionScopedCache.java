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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.UnsafeArg;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ValidatingTransactionScopedCache implements TransactionScopedCache {
    private static final Logger log = LoggerFactory.getLogger(ValidatingTransactionScopedCache.class);
    private static final double VALIDATION_PROBABILITY = 1.0;

    private final TransactionScopedCache delegate;
    private final Supplier<Boolean> validationSupplier;

    @VisibleForTesting
    ValidatingTransactionScopedCache(TransactionScopedCache delegate, Supplier<Boolean> validationSupplier) {
        this.delegate = delegate;
        this.validationSupplier = validationSupplier;
    }

    static ValidatingTransactionScopedCache create(TransactionScopedCache delegate) {
        return new ValidatingTransactionScopedCache(
                delegate, () -> ThreadLocalRandom.current().nextDouble() < VALIDATION_PROBABILITY);
    }

    @Override
    public void write(TableReference tableReference, Map<Cell, byte[]> values) {
        delegate.write(tableReference, values);
    }

    @Override
    public void delete(TableReference tableReference, Set<Cell> cells) {
        delegate.delete(tableReference, cells);
    }

    @Override
    public Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cell,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        if (validationSupplier.get()) {
            Map<Cell, byte[]> remoteReads = AtlasFutures.getUnchecked(valueLoader.apply(tableReference, cell));
            Map<Cell, byte[]> cacheReads =
                    delegate.get(tableReference, cell, (table, cells) -> getCells(remoteReads, cells));

            validateCacheReads(tableReference, remoteReads, cacheReads);
            return cacheReads;
        } else {
            return delegate.get(tableReference, cell, valueLoader);
        }
    }

    @Override
    public ValueDigest getValueDigest() {
        return delegate.getValueDigest();
    }

    @Override
    public HitDigest getHitDigest() {
        return delegate.getHitDigest();
    }

    private static ListenableFuture<Map<Cell, byte[]>> getCells(Map<Cell, byte[]> remoteReads, Set<Cell> cells) {
        return Futures.immediateFuture(KeyedStream.of(cells)
                .map(remoteReads::get)
                .filter(Objects::nonNull)
                .collectToMap());
    }

    private static void validateCacheReads(
            TableReference tableReference, Map<Cell, byte[]> remoteReads, Map<Cell, byte[]> cacheReads) {
        if (!remoteReads.equals(cacheReads)) {
            log.error(
                    "Reading from lock watch cache returned a different result to a remote read - this indicates there "
                            + "is a corruption bug in the caching logic",
                    UnsafeArg.of("table", tableReference),
                    UnsafeArg.of("remoteReads", remoteReads),
                    UnsafeArg.of("cacheReads", cacheReads));
            throw new TransactionFailedNonRetriableException("Failed lock watch cache validation");
        }
    }
}
