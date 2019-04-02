/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.CheckForNull;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

/**
 * This class coalesces write (that is, put-unless-exists) requests to an underlying {@link EncodingTransactionService},
 * such that there is at most one request in flight at a given time. Read requests (gets) are not batched.
 *
 * Delegates are expected to throw {@link KeyAlreadyExistsException}s that have meaningful values for
 * {@link KeyAlreadyExistsException#getExistingKeys()}.
 */
public final class WriteBatchingTransactionService implements TransactionService {
    private final EncodingTransactionService delegate;
    private final DisruptorAutobatcher<TimestampPair, Void> autobatcher;

    private WriteBatchingTransactionService(
            EncodingTransactionService delegate, DisruptorAutobatcher<TimestampPair, Void> autobatcher) {
        this.delegate = delegate;
        this.autobatcher = autobatcher;
    }

    public static TransactionService create(EncodingTransactionService delegate) {
        DisruptorAutobatcher<TimestampPair, Void> autobatcher = DisruptorAutobatcher.create(
                elements -> processBatch(delegate, elements));
        return new WriteBatchingTransactionService(delegate, autobatcher);
    }

    @CheckForNull
    @Override
    public Long get(long startTimestamp) {
        return delegate.get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return delegate.get(startTimestamps);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        try {
            autobatcher.apply(TimestampPair.of(startTimestamp, commitTimestamp)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    @Override
    public void close() {
        autobatcher.close();
        delegate.close();
    }

    /**
     * Semantics for batch processing:
     *
     * - If there are multiple requests to {@link TransactionService#putUnlessExists(long, long)} with the same
     *   start timestamp, only the first of these will actually be attempted; the remainder will receive a response
     *   indicating they should try again. The booleans returned by this method indicate whether a request to the
     *   batcher that has been processed was processed properly (true), or if it should be re-submitted (false).
     * - If a {@link KeyAlreadyExistsException} is thrown, we fail out requests for keys present in the
     *   {@link KeyAlreadyExistsException}, and then retry our request with those keys removed. If the
     *   {@link KeyAlreadyExistsException} does not include any present keys, we throw an exception.
     *
     * Retrying does theoretically mean that in the worst case with N transactions in our batch, we may actually
     * require N calls to the database, though this is extremely unlikely especially because of the semantics of
     * {@link TimelockService#startIdentifiedAtlasDbTransaction(StartIdentifiedAtlasDbTransactionRequest)}.
     * Alternatives considered included failing out all requests (which is likely to be inefficient and lead to
     * spurious retries on requests that actually committed), and re-submitting requests other than the failed one
     * for consideration in the next batch (which may achieve higher throughput, but could lead to starvation of old
     * requests).
     */
    @VisibleForTesting
    static void processBatch(
            EncodingTransactionService delegate, List<BatchElement<TimestampPair, Void>> batchElements) {
        Multimap<Long, BatchElement<TimestampPair, Void>> startTimestampKeyedBatchElements
                = MultimapBuilder.hashKeys().hashSetValues().build();
        batchElements.forEach(batchElement -> startTimestampKeyedBatchElements.put(
                batchElement.argument().startTimestamp(),
                batchElement));

        while (!startTimestampKeyedBatchElements.isEmpty()) {
            Map<Long, BatchElement<TimestampPair, Void>> batch =
                    extractSingleBatchForQuerying(startTimestampKeyedBatchElements);
            try {
                delegate.putUnlessExistsMultiple(KeyedStream.stream(batch)
                        .map(batchElement -> batchElement.argument().commitTimestamp())
                        .collectToMap());
                markBatchSuccessful(startTimestampKeyedBatchElements, batch);
            } catch (KeyAlreadyExistsException exception) {
                Set<Long> failedTimestamps = getAlreadyExistingStartTimestamps(delegate, batch.keySet(), exception);
                failedTimestamps.forEach(timestamp -> {
                    BatchElement<TimestampPair, Void> batchElement = batch.get(timestamp);
                    batchElement.result().setException(exception);
                    startTimestampKeyedBatchElements.remove(timestamp, batchElement);
                });

                Set<Long> successfulTimestamps = getTimestampsSuccessfullyPutUnlessExists(delegate, exception);
                successfulTimestamps.forEach(timestamp -> {
                    BatchElement<TimestampPair, Void> batchElement = batch.get(timestamp);
                    markSuccessful(batchElement.result());
                    startTimestampKeyedBatchElements.remove(timestamp, batchElement);
                });
            }
        }
    }

    private static void markBatchSuccessful(
            Multimap<Long, BatchElement<TimestampPair, Void>> startTimestampKeyedBatchElements,
            Map<Long, BatchElement<TimestampPair, Void>> batch) {
        batch.forEach((startTimestamp, batchElement) -> {
            startTimestampKeyedBatchElements.remove(startTimestamp, batchElement);
            markSuccessful(batchElement.result());
        });
    }

    private static void markSuccessful(SettableFuture<Void> result) {
        result.set(null);
    }

    private static Set<Long> getAlreadyExistingStartTimestamps(
            EncodingTransactionService delegate,
            Set<Long> startTimestamps,
            KeyAlreadyExistsException exception) {
        Set<Long> existingTimestamps = decodeCellsToTimestamps(delegate, exception.getExistingKeys());
        Preconditions.checkState(!existingTimestamps.isEmpty(),
                "The underlying service threw a KeyAlreadyExistsException, but claimed no keys already existed!"
                        + " This is likely to be a product bug - please contact support.",
                SafeArg.of("startTimestamps", startTimestamps),
                UnsafeArg.of("exception", exception));
        return existingTimestamps;
    }

    private static Set<Long> getTimestampsSuccessfullyPutUnlessExists(
            EncodingTransactionService delegate,
            KeyAlreadyExistsException exception) {
        return decodeCellsToTimestamps(delegate, exception.getKnownSuccessfullyCommittedKeys());
    }

    private static Set<Long> decodeCellsToTimestamps(EncodingTransactionService delegate, Collection<Cell> cells) {
        return cells.stream()
                .map(key -> delegate.getEncodingStrategy().decodeCellAsStartTimestamp(key))
                .collect(Collectors.toSet());
    }

    private static Map<Long, BatchElement<TimestampPair, Void>> extractSingleBatchForQuerying(
            Multimap<Long, BatchElement<TimestampPair, Void>> requests) {
        Map<Long, BatchElement<TimestampPair, Void>> result = Maps.newHashMapWithExpectedSize(requests.keySet().size());
        for (Map.Entry<Long, Collection<BatchElement<TimestampPair, Void>>> entry
                : Multimaps.asMap(requests).entrySet()) {
            result.put(entry.getKey(), entry.getValue().iterator().next());
        }
        return result;
    }

    @Value.Immutable
    interface TimestampPair {
        @Value.Parameter
        long startTimestamp();

        @Value.Parameter
        long commitTimestamp();

        static TimestampPair of(long startTimestamp, long commitTimestamp) {
            return ImmutableTimestampPair.of(startTimestamp, commitTimestamp);
        }
    }
}
