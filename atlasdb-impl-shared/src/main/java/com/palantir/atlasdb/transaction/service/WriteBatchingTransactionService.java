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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class coalesces write (that is, put-unless-exists) requests to an underlying {@link EncodingTransactionService},
 * such that there is at most one request in flight at a given time. Read requests (gets) are not batched.
 *
 * Delegates are expected to throw {@link KeyAlreadyExistsException}s that have meaningful values for
 * {@link KeyAlreadyExistsException#getExistingKeys()}.
 */
public final class WriteBatchingTransactionService implements TransactionService {
    private static final Logger log = LoggerFactory.getLogger(WriteBatchingTransactionService.class);

    private final EncodingTransactionService delegate;
    private final DisruptorAutobatcher<TimestampPair, Void> autobatcher;

    private WriteBatchingTransactionService(
            EncodingTransactionService delegate, DisruptorAutobatcher<TimestampPair, Void> autobatcher) {
        this.delegate = delegate;
        this.autobatcher = autobatcher;
    }

    public static TransactionService create(EncodingTransactionService delegate) {
        DisruptorAutobatcher<TimestampPair, Void> autobatcher = Autobatchers
                .<TimestampPair, Void>independent(elements -> processBatch(delegate, elements))
                .safeLoggablePurpose("write-batching-transaction-service")
                .build();
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
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return delegate.getAsync(startTimestamp);
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return delegate.getAsync(startTimestamps);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        AtlasFutures.getUnchecked(autobatcher.apply(TimestampPair.of(startTimestamp, commitTimestamp)));
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
     *   start timestamp, we will actually call the KVS with only one request from the batch for that start timestamp.
     *   There are no guarantees as to which request we use. If that element was successfully put (if the whole
     *   operation succeeded, or if the {@link KeyAlreadyExistsException} has partial successes), we return
     *   success for that request, and throw {@link KeyAlreadyExistsException} for all other requests associated with
     *   that start timestamp. If it failed and we know the key already exists, all requests associated with that
     *   start timestamp are failed with a {@link KeyAlreadyExistsException}. Otherwise we will try again.
     * - If a {@link KeyAlreadyExistsException} is thrown, we fail out requests for keys present in the
     *   {@link KeyAlreadyExistsException}, and then retry our request with those keys removed. If the
     *   {@link KeyAlreadyExistsException} does not include any present keys, we throw an exception.
     *
     * Retrying does theoretically mean that in the worst case with N transactions in our batch, we may actually
     * require N calls to the database, though this is extremely unlikely especially because of the semantics of
     * {@link TimelockService#startIdentifiedAtlasDbTransactionBatch(int)}.
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
                markAllRemainingRequestsAsFailed(delegate, startTimestampKeyedBatchElements.values());
                return;
            } catch (KeyAlreadyExistsException exception) {
                handleFailedTimestamps(delegate, startTimestampKeyedBatchElements, batch, exception);
                handleSuccessfulTimestamps(delegate, startTimestampKeyedBatchElements, batch, exception);
            }
        }
    }

    private static void markAllRemainingRequestsAsFailed(
            EncodingTransactionService delegate,
            Collection<BatchElement<TimestampPair, Void>> batchElementsToFail) {
        batchElementsToFail.forEach(batchElem -> markPreemptivelyAsFailure(delegate, batchElem));
    }

    private static void markPreemptivelyAsFailure(
            EncodingTransactionService delegate,
            BatchElement<TimestampPair, Void> batchElem) {
        Cell cell = delegate.getEncodingStrategy().encodeStartTimestampAsCell(batchElem.argument().startTimestamp());
        KeyAlreadyExistsException exception = new KeyAlreadyExistsException(
                "Failed because other client-side element succeeded",
                ImmutableList.of(cell));
        batchElem.result().setException(exception);
    }

    private static void handleFailedTimestamps(EncodingTransactionService delegate,
            Multimap<Long, BatchElement<TimestampPair, Void>> startTimestampKeyedBatchElements,
            Map<Long, BatchElement<TimestampPair, Void>> batch,
            KeyAlreadyExistsException exception) {
        Set<Long> failedTimestamps = getAlreadyExistingStartTimestamps(delegate, batch.keySet(), exception);
        Map<Boolean, List<Long>> wereFailedTimestampsExpected = classifyTimestampsOnKeySetPresence(
                batch.keySet(), failedTimestamps);
        handleExpectedFailedTimestamps(batch, exception, failedTimestamps,
                wereFailedTimestampsExpected);
        handleUnexpectedFailedTimestamps(batch, wereFailedTimestampsExpected);

        markAllRemainingRequestsForTimestampsAsFailed(delegate, startTimestampKeyedBatchElements, failedTimestamps);
    }

    private static void handleExpectedFailedTimestamps(
            Map<Long, BatchElement<TimestampPair, Void>> batch,
            KeyAlreadyExistsException exception, Set<Long> failedTimestamps,
            Map<Boolean, List<Long>> wereFailedTimestampsExpected) {
        List<Long> expectedFailedTimestamps = wereFailedTimestampsExpected.get(true);
        if (expectedFailedTimestamps == null) {
            // We expected something important to fail, but nothing did.
            throw new SafeIllegalStateException("Made a batch request to putUnlessExists and failed."
                    + " The exception returned did not contain anything pertinent, which is unexpected.",
                    SafeArg.of("batchRequest", batch),
                    SafeArg.of("failedTimestamps", failedTimestamps));
        }
        expectedFailedTimestamps.forEach(timestamp -> {
            BatchElement<TimestampPair, Void> batchElement = batch.get(timestamp);
            batchElement.result().setException(exception);
        });
    }

    private static void handleUnexpectedFailedTimestamps(
            Map<Long, BatchElement<TimestampPair, Void>> batch,
            Map<Boolean, List<Long>> wereFailedTimestampsExpected) {
        List<Long> unexpectedFailedTimestamps = wereFailedTimestampsExpected.get(false);
        if (unexpectedFailedTimestamps != null) {
            log.warn("Failed to putUnlessExists some timestamps which it seems we never asked for."
                            + " Skipping, as this is likely to be safe, but flagging for debugging.",
                    SafeArg.of("unexpectedFailedTimestamps", unexpectedFailedTimestamps),
                    SafeArg.of("batchRequest", batch));
        }
    }

    private static void handleSuccessfulTimestamps(EncodingTransactionService delegate,
            Multimap<Long, BatchElement<TimestampPair, Void>> startTimestampKeyedBatchElements,
            Map<Long, BatchElement<TimestampPair, Void>> batch,
            KeyAlreadyExistsException exception) {
        Set<Long> successfulTimestamps = getTimestampsSuccessfullyPutUnlessExists(delegate, exception);
        Map<Boolean, List<Long>> wereSuccessfulTimestampsExpected
                = classifyTimestampsOnKeySetPresence(batch.keySet(), successfulTimestamps);
        handleExpectedSuccesses(batch, wereSuccessfulTimestampsExpected);
        handleUnexpectedSuccesses(batch, wereSuccessfulTimestampsExpected);

        markAllRemainingRequestsForTimestampsAsFailed(delegate, startTimestampKeyedBatchElements, successfulTimestamps);
    }

    private static void markAllRemainingRequestsForTimestampsAsFailed(EncodingTransactionService delegate,
            Multimap<Long, BatchElement<TimestampPair, Void>> startTimestampKeyedBatchElements,
            Set<Long> timestampsToFail) {
        timestampsToFail.stream()
                .map(startTimestampKeyedBatchElements::get)
                .forEach(batchElements -> markAllRemainingRequestsAsFailed(delegate, batchElements));
        timestampsToFail.forEach(startTimestampKeyedBatchElements::removeAll);
    }

    private static void handleExpectedSuccesses(
            Map<Long, BatchElement<TimestampPair, Void>> batch,
            Map<Boolean, List<Long>> wereSuccessfulTimestampsExpected) {
        List<Long> expectedSuccessfulTimestamps = wereSuccessfulTimestampsExpected.get(true);
        if (expectedSuccessfulTimestamps != null) {
            expectedSuccessfulTimestamps.forEach(timestamp -> {
                BatchElement<TimestampPair, Void> batchElement = batch.get(timestamp);
                markSuccessful(batchElement.result());
            });
        }
    }

    private static void handleUnexpectedSuccesses(
            Map<Long, BatchElement<TimestampPair, Void>> batch,
            Map<Boolean, List<Long>> wereSuccessfulTimestampsExpected) {
        List<Long> unexpectedSuccessfulTimestamps = wereSuccessfulTimestampsExpected.get(false);
        if (unexpectedSuccessfulTimestamps != null) {
            log.warn("Successfully putUnlessExists some timestamps which it seems we never asked for."
                            + " Skipping, as this is likely to be safe, but flagging for debugging.",
                    SafeArg.of("unexpectedSuccessfulPuts", unexpectedSuccessfulTimestamps),
                    SafeArg.of("batchRequest", batch));
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

    private static void markSuccessful(DisruptorAutobatcher.DisruptorFuture<Void> result) {
        result.set(null);
    }

    private static Map<Boolean, List<Long>> classifyTimestampsOnKeySetPresence(
            Set<Long> requestKeySet,
            Set<Long> timestampSet) {
        return timestampSet.stream().collect(Collectors.groupingBy(requestKeySet::contains));
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
