/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client.timestampleases;

import com.codahale.metrics.Counter;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.LeaseGuarantee;
import com.palantir.atlasdb.timelock.api.LeaseIdentifier;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.RequestId;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.atlasdb.timelock.api.TimestampLeaseRequests;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponses;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.ConjureTimestampRangeTimestampSupplier;
import com.palantir.lock.LimitingLongSupplier;
import com.palantir.lock.client.LeasedLockToken;
import com.palantir.lock.client.LockTokenUnlocker;
import com.palantir.lock.client.TimestampLeaseMetrics;
import com.palantir.lock.v2.TimestampLeaseResult;
import com.palantir.lock.v2.TimestampLeaseResults;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.ids.UniqueIds;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class TimestampLeaseAcquirerImpl implements TimestampLeaseAcquirer {
    private static final SafeLogger log = SafeLoggerFactory.get(TimestampLeaseAcquirerImpl.class);

    private final NamespacedTimestampLeaseService delegate;
    private final Unlocker unlocker;
    private final Supplier<UUID> uuidSupplier;
    private final Counter notEnoughFreshTimestampsCounter;

    private final Retryer<Optional<TimestampLeaseResponses>> retryer =
            RetryerBuilder.<Optional<TimestampLeaseResponses>>newBuilder()
                    .retryIfResult(Optional::isEmpty)
                    .withStopStrategy(StopStrategies.stopAfterAttempt(3))
                    .build();

    private TimestampLeaseAcquirerImpl(
            NamespacedTimestampLeaseService delegate,
            Unlocker unlocker,
            Supplier<UUID> uuidSupplier,
            Counter notEnoughFreshTimestampsCounter) {
        this.delegate = delegate;
        this.unlocker = unlocker;
        this.uuidSupplier = uuidSupplier;
        this.notEnoughFreshTimestampsCounter = notEnoughFreshTimestampsCounter;
    }

    @VisibleForTesting
    TimestampLeaseAcquirerImpl(
            NamespacedTimestampLeaseService delegate, Unlocker unlocker, Supplier<UUID> uuidSupplier) {
        this(delegate, unlocker, uuidSupplier, new Counter());
    }

    public static TimestampLeaseAcquirer create(
            NamespacedTimestampLeaseService delegate, LockTokenUnlocker unlocker, TimestampLeaseMetrics metrics) {
        return new TimestampLeaseAcquirerImpl(
                delegate,
                identifier -> unlock(unlocker, identifier),
                UniqueIds::pseudoRandomUuidV4,
                metrics.notEnoughFreshTimestamps());
    }

    @Override
    public TimestampLeaseResults acquireNamedTimestampLeases(Map<TimestampLeaseName, Integer> requests) {
        TimestampLeaseResponses response = acquireLeasesWithRetry(requests);
        try {
            return TimestampLeaseResults.builder()
                    .lock(createLeasedLockToken(response))
                    .results(createTimestampLeaseResult(requests, response.getTimestampLeaseResponses()))
                    .build();
        } catch (RuntimeException | Error e) {
            log.error("Unexpected exception while creating client results", e);
            unlock(response);
            throw e;
        }
    }

    @Override
    public void close() {
        // TODO(aalouane): decide whether or not to close the unlocker depending on ownership
    }

    private TimestampLeaseResponses acquireLeasesWithRetry(Map<TimestampLeaseName, Integer> requests) {
        try {
            return retryer.call(() -> acquireLeases(requests)).orElseThrow();
        } catch (ExecutionException e) {
            // should not happen with how the retryer is set up
            log.warn("Unexpected exception. Expected a retry exception", e);
            throw new SafeRuntimeException(e);
        } catch (RetryException e) {
            Attempt<?> lastFailedAttempt = e.getLastFailedAttempt();
            log.warn(
                    "Exhausted retries to get enough timestamps while acquiring timestamp lease",
                    SafeArg.of("requests", requests),
                    SafeArg.of("numRetries", lastFailedAttempt.getAttemptNumber()),
                    SafeArg.of("attemptHadException", lastFailedAttempt.hasException()),
                    e);
            throw new AtlasDbDependencyException(
                    "Exhausted retries to get enough timestamps while acquiring timestamp lease",
                    lastFailedAttempt.hasException() ? lastFailedAttempt.getExceptionCause() : null,
                    SafeArg.of("requests", requests),
                    SafeArg.of("numRetries", lastFailedAttempt.getAttemptNumber()),
                    SafeArg.of(
                            "maybeResult",
                            lastFailedAttempt.hasResult() ? lastFailedAttempt.getResult() : Optional.empty()));
        }
    }

    private Optional<TimestampLeaseResponses> acquireLeases(Map<TimestampLeaseName, Integer> requestedFreshTimestamps) {
        // we prefer to use a new request id for non-dialogue-native dialogue attempts
        TimestampLeaseRequests request =
                TimestampLeaseRequests.of(RequestId.of(uuidSupplier.get()), requestedFreshTimestamps);
        NamespaceTimestampLeaseRequest requests = NamespaceTimestampLeaseRequest.of(List.of(request));

        TimestampLeaseResponses response = Iterables.getOnlyElement(
                delegate.acquireTimestampLeases(requests).get());
        Map<TimestampLeaseName, TimestampLeaseResponse> responseMap = response.getTimestampLeaseResponses();

        Preconditions.checkArgument(
                requestedFreshTimestamps.keySet().equals(responseMap.keySet()),
                "Response lease timestamps need to match request timestamp names exactly");

        long wereNotFullyFulfilled = requestedFreshTimestamps.keySet().stream()
                .filter(timestampName -> {
                    int requestedTimestamps = requestedFreshTimestamps.get(timestampName);
                    long returnedTimestamps =
                            responseMap.get(timestampName).getFreshTimestamps().getCount();
                    return returnedTimestamps < requestedTimestamps;
                })
                .count();

        if (wereNotFullyFulfilled > 0) {
            notEnoughFreshTimestampsCounter.inc(wereNotFullyFulfilled);

            unlock(response);
            log.info(
                    "Timestamp lease request was not fully fulfilled. This should happen infrequently.",
                    SafeArg.of("requests", request),
                    SafeArg.of("responses", response));
            return Optional.empty();
        } else {
            return Optional.of(response);
        }
    }

    private void unlock(TimestampLeaseResponses responses) {
        unlocker.unlock(responses.getLeaseGuarantee().getIdentifier());
    }

    private static void unlock(LockTokenUnlocker unlocker, LeaseIdentifier leaseGuarantee) {
        unlocker.unlock(Set.of(ConjureLockTokenV2.of(leaseGuarantee.get())));
    }

    private static Map<TimestampLeaseName, TimestampLeaseResult> createTimestampLeaseResult(
            Map<TimestampLeaseName, Integer> requestedTimestamps,
            Map<TimestampLeaseName, TimestampLeaseResponse> responses) {
        return KeyedStream.stream(responses)
                .<TimestampLeaseResult>map((timestampName, response) -> {
                    int requestedForName = requestedTimestamps.get(timestampName);
                    LongSupplier freshTimestamps = new LimitingLongSupplier(
                            new ConjureTimestampRangeTimestampSupplier(response.getFreshTimestamps()),
                            requestedForName);
                    return TimestampLeaseResult.builder()
                            .minLeasedTimestamp(response.getMinLeased())
                            .freshTimestampsSupplier(freshTimestamps)
                            .build();
                })
                .collectToMap();
    }

    private static LeasedLockToken createLeasedLockToken(TimestampLeaseResponses responses) {
        LeaseGuarantee leaseGuarantee = responses.getLeaseGuarantee();
        return LeasedLockToken.of(
                ConjureLockToken.of(leaseGuarantee.getIdentifier().get()), leaseGuarantee.getLease());
    }

    interface Unlocker {
        void unlock(LeaseIdentifier leaseGuarantee);
    }
}
