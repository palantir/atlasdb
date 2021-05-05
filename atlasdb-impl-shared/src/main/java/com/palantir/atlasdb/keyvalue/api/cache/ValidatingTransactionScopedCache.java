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
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.UnsafeArg;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ValidatingTransactionScopedCache implements TransactionScopedCache {
    private static final Logger log = LoggerFactory.getLogger(ValidatingTransactionScopedCache.class);

    private final TransactionScopedCache delegate;
    private double validationProbability;
    private final Random random;

    @VisibleForTesting
    ValidatingTransactionScopedCache(TransactionScopedCache delegate, double validationProbability) {
        this.delegate = delegate;
        this.validationProbability = validationProbability;
        this.random = new Random();
    }

    static ValidatingTransactionScopedCache create(TransactionScopedCache delegate, double validationProbability) {
        return new ValidatingTransactionScopedCache(delegate, validationProbability);
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
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        return AtlasFutures.getUnchecked(getAsync(tableReference, cells, valueLoader));
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getAsync(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        if (shouldValidate()) {
            ListenableFuture<Map<Cell, byte[]>> remoteReads = valueLoader.apply(tableReference, cells);
            ListenableFuture<Map<Cell, byte[]>> cacheReads = delegate.getAsync(
                    tableReference,
                    cells,
                    (table, cellsToRead) -> Futures.transform(
                            remoteReads, reads -> getCells(reads, cellsToRead), MoreExecutors.directExecutor()));

            return Futures.transform(
                    cacheReads,
                    reads -> {
                        validateCacheReads(tableReference, AtlasFutures.getDone(remoteReads), reads);
                        return reads;
                    },
                    MoreExecutors.directExecutor());
        } else {
            return delegate.getAsync(tableReference, cells, valueLoader);
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public ValueDigest getValueDigest() {
        return delegate.getValueDigest();
    }

    private boolean shouldValidate() {
        return random.nextDouble() < validationProbability;
    }

    private void validateCacheReads(
            TableReference tableReference, Map<Cell, byte[]> remoteReads, Map<Cell, byte[]> cacheReads) {
        if (!remoteReads.equals(cacheReads)) {
            // TODO(jshah): make sure that this causes us to disable all caching until restart
            log.error(
                    "Reading from lock watch cache returned a different result to a remote read - this indicates there "
                            + "is a corruption bug in the caching logic",
                    UnsafeArg.of("table", tableReference),
                    UnsafeArg.of("remoteReads", remoteReads),
                    UnsafeArg.of("cacheReads", cacheReads));
            throw new TransactionFailedNonRetriableException("Failed lock watch cache validation");
        }
    }

    private static Map<Cell, byte[]> getCells(Map<Cell, byte[]> remoteReads, Set<Cell> cells) {
        return KeyedStream.of(cells)
                .map(remoteReads::get)
                .filter(Objects::nonNull)
                .collectToMap();
    }
}
