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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.common.annotation.Output;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

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
     *   start timestamp, only the first of these will actually be attempted; the remainder will fail with a
     *   {@link SafeIllegalArgumentException}, which is fine (following the contract it means the put may or may not
     *   have happened).
     * - If a {@link KeyAlreadyExistsException} is thrown, we fail out requests for keys present in the
     *   {@link KeyAlreadyExistsException}, and then retry our request with those keys removed. If the
     *   {@link KeyAlreadyExistsException} does not include any present keys, we throw an exception.
     *
     * Retrying does theoretically mean that in the worst case with N transactions in our batch, we may actually
     * require N calls to the database, though this is extremely unlikely especially because of the semantics of
     * {@link TimelockService#startIdentifiedAtlasDbTransaction()}.
     * Alternatives considered included failing out all requests (which is likely to be inefficient and lead to
     * spurious retries on requests that actually committed), and re-submitting requests other than the failed one
     * for consideration in the next batch (which may achieve higher throughput, but could lead to starvation of old
     * requests).
     */
    @VisibleForTesting
    static void processBatch(
            EncodingTransactionService delegate, List<BatchElement<TimestampPair, Void>> batchElements) {
        Map<Long, Long> accumulatedRequest = Maps.newHashMap();
        Map<Long, SettableFuture<Void>> futures = Maps.newHashMap();
        failDuplicateRequestsOnStartTimestamp(batchElements, accumulatedRequest, futures);

        while (!accumulatedRequest.isEmpty()) {
            try {
                delegate.putUnlessExistsMultiple(accumulatedRequest);

                // If the result was already set to be exceptional, this will not interfere with that.
                batchElements.forEach(element -> element.result().set(null));
                return;
            } catch (KeyAlreadyExistsException exception) {
                Map<Long, Long> thisRequest = ImmutableMap.copyOf(accumulatedRequest);

                Set<Long> failedTimestamps = extractAndHandleFailures(delegate, accumulatedRequest, futures, exception,
                        thisRequest);

                Set<Long> successfulTimestamps = extractAndHandleExistingSuccesses(delegate, futures, exception,
                        thisRequest);

                accumulatedRequest = Maps.filterKeys(accumulatedRequest,
                        timestamp -> !failedTimestamps.contains(timestamp)
                                && !successfulTimestamps.contains(timestamp));
            }
        }
    }

    private static Set<Long> extractAndHandleFailures(EncodingTransactionService delegate,
            Map<Long, Long> accumulatedRequest, Map<Long, SettableFuture<Void>> futures,
            KeyAlreadyExistsException exception, Map<Long, Long> thisRequest) {
        Set<Long> failedTimestamps = getAlreadyExistingStartTimestamps(delegate, accumulatedRequest, exception);

        Map<Boolean, List<Long>> wereFailedTimestampsExpected = classifyTimestampsOnKeySetPresence(
                thisRequest, failedTimestamps);

        handleExpectedFailures(futures, exception, thisRequest, failedTimestamps, wereFailedTimestampsExpected);
        handleUnexpectedFailures(thisRequest, wereFailedTimestampsExpected);
        return failedTimestamps;
    }

    private static Map<Boolean, List<Long>> classifyTimestampsOnKeySetPresence(Map<Long, Long> requestKeySet,
            Set<Long> timestampSet) {
        return timestampSet.stream().collect(Collectors.groupingBy(requestKeySet::containsKey));
    }

    private static void handleExpectedFailures(Map<Long, SettableFuture<Void>> futures,
            KeyAlreadyExistsException exception, Map<Long, Long> thisRequest, Set<Long> failedTimestamps,
            Map<Boolean, List<Long>> wereFailedTimestampsExpected) {
        List<Long> expectedFailedTimestamps = wereFailedTimestampsExpected.get(true);
        if (expectedFailedTimestamps == null) {
            // We expected something important to fail, but nothing did.
            throw new SafeIllegalStateException("Made a batch request to putUnlessExists and failed."
                    + " The exception returned did not contain anything pertinent, which is unexpected.",
                    SafeArg.of("batchRequest", thisRequest),
                    SafeArg.of("failedTimestamps", failedTimestamps));
        }
        expectedFailedTimestamps.forEach(timestamp -> futures.get(timestamp).setException(exception));
    }

    private static void handleUnexpectedFailures(Map<Long, Long> thisRequest,
            Map<Boolean, List<Long>> wereFailedTimestampsExpected) {
        List<Long> unexpectedFailedTimestamps = wereFailedTimestampsExpected.get(false);
        if (unexpectedFailedTimestamps != null) {
            log.warn("Failed to putUnlessExists some timestamps which it seems we never asked for."
                            + " Skipping, as this is likely to be safe, but flagging for debugging.",
                    SafeArg.of("unexpectedFailedTimestamps", unexpectedFailedTimestamps),
                    SafeArg.of("batchRequest", thisRequest));
        }
    }

    private static Set<Long> extractAndHandleExistingSuccesses(EncodingTransactionService delegate,
            Map<Long, SettableFuture<Void>> futures, KeyAlreadyExistsException exception, Map<Long, Long> thisRequest) {
        Set<Long> successfulTimestamps = getTimestampsSuccessfullyPutUnlessExists(delegate, exception);
        Map<Boolean, List<Long>> wereSuccessfulTimestampsExpected = classifyTimestampsOnKeySetPresence(
                thisRequest, successfulTimestamps);
        handleExpectedSuccesses(futures, wereSuccessfulTimestampsExpected);
        handleUnexpectedSuccesses(thisRequest, wereSuccessfulTimestampsExpected);
        return successfulTimestamps;
    }

    private static void handleExpectedSuccesses(Map<Long, SettableFuture<Void>> futures,
            Map<Boolean, List<Long>> wereSuccessfulTimestampsExpected) {
        wereSuccessfulTimestampsExpected.getOrDefault(true, ImmutableList.of()).forEach(
                timestamp -> futures.get(timestamp).set(null));
    }

    private static void handleUnexpectedSuccesses(Map<Long, Long> thisRequest,
            Map<Boolean, List<Long>> wereSuccessfulTimestampsExpected) {
        List<Long> unexpectedlySuccessfulTimestamps = wereSuccessfulTimestampsExpected.get(false);
        if (unexpectedlySuccessfulTimestamps != null) {
            log.warn("Successfully putUnlessExists some timestamps which it seems we never asked for."
                            + " Skipping, as this is likely to be safe, but flagging for debugging.",
                    SafeArg.of("unexpectedSuccessfulPuts", unexpectedlySuccessfulTimestamps),
                    SafeArg.of("batchRequest", thisRequest));
        }
    }

    private static Set<Long> getAlreadyExistingStartTimestamps(
            EncodingTransactionService delegate,
            Map<Long, Long> batchRequest,
            KeyAlreadyExistsException exception) {
        Set<Long> existingTimestamps = decodeCellsToTimestamps(delegate, exception.getExistingKeys());
        Preconditions.checkState(!existingTimestamps.isEmpty(),
                "The underlying service threw a KeyAlreadyExistsException, but claimed no keys already existed!"
                        + " This is likely to be a product bug - please contact support.",
                SafeArg.of("request", batchRequest),
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

    private static void failDuplicateRequestsOnStartTimestamp(
            List<BatchElement<TimestampPair, Void>> batchElements,
            @Output Map<Long, Long> accumulatedRequest,
            @Output Map<Long, SettableFuture<Void>> futures) {
        for (BatchElement<TimestampPair, Void> batchElement : batchElements) {
            TimestampPair pair = batchElement.argument();
            if (!accumulatedRequest.containsKey(pair.startTimestamp())) {
                accumulatedRequest.put(pair.startTimestamp(), pair.commitTimestamp());
                futures.put(pair.startTimestamp(), batchElement.result());
            } else {
                batchElement.result().setException(
                        new SafeIllegalArgumentException("Attempted to putUnlessExists the same start timestamp as"
                                + " another request which came first, so we will prioritise that.",
                                SafeArg.of("startTimestamp", pair.startTimestamp()),
                                SafeArg.of("commitTimestamp", pair.commitTimestamp())));
            }
        }
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
