/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.net.SocketTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Ensures that clients actually attempt to acquire the lock for the full duration they claim they will block for,
 * unless they run into an exception that is unlikely to actually be a timeout. This is done to account for the reality
 * of bounded timeouts beneath us (e.g. in terms of the networking layer) when communicating with remote services.
 *
 * Fairness is admittedly compromised, but this is a closer approximation than the previous behaviour.
 */
final class BlockEnforcingLockService {
    private final NamespacedConjureTimelockService namespacedConjureTimelockService;
    private final RemoteTimeoutRetryer timeoutRetryer;

    private BlockEnforcingLockService(NamespacedConjureTimelockService namespacedConjureTimelockService,
            RemoteTimeoutRetryer timeoutRetryer) {
        this.namespacedConjureTimelockService = namespacedConjureTimelockService;
        this.timeoutRetryer = timeoutRetryer;
    }

    static BlockEnforcingLockService create(NamespacedConjureTimelockService namespacedConjureTimelockService) {
        return new BlockEnforcingLockService(namespacedConjureTimelockService, RemoteTimeoutRetryer.createDefault());
    }

    LockResponse lock(LockRequest request) {
        // The addition of a UUID takes place only at the Conjure level, so we must retry the same request.
        return timeoutRetryer.attemptUntilTimeLimitOrException(
                ConjureLockRequests.toConjure(request),
                Duration.ofMillis(request.getAcquireTimeoutMs()),
                BlockEnforcingLockService::clampLockRequestToDeadline,
                this::performSingleLockRequest,
                response -> !response.wasSuccessful());
    }

    WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return timeoutRetryer.attemptUntilTimeLimitOrException(
                ConjureLockRequests.toConjure(request),
                Duration.ofMillis(request.getAcquireTimeoutMs()),
                BlockEnforcingLockService::clampLockRequestToDeadline,
                this::performSingleWaitForLocksRequest,
                response -> !response.wasSuccessful());
    }

    private static ConjureLockRequest clampLockRequestToDeadline(ConjureLockRequest request, Duration remainingTime) {
        return ConjureLockRequest.builder()
                .from(request)
                .acquireTimeoutMs(Ints.checkedCast(remainingTime.toMillis()))
                .build();
    }

    private LockResponse performSingleLockRequest(ConjureLockRequest request) {
        return namespacedConjureTimelockService
                .lock(request)
                .accept(ToLeasedLockResponse.INSTANCE);
    }

    private WaitForLocksResponse performSingleWaitForLocksRequest(ConjureLockRequest request) {
        return ConjureLockRequests.fromConjure(namespacedConjureTimelockService.waitForLocks(request));
    }

    private enum ToLeasedLockResponse implements ConjureLockResponse.Visitor<LockResponse> {
        INSTANCE;

        @Override
        public LockResponse visitSuccessful(SuccessfulLockResponse value) {
            return LockResponse.successful(LeasedLockToken.of(value.getLockToken(), value.getLease()));
        }

        @Override
        public LockResponse visitUnsuccessful(UnsuccessfulLockResponse value) {
            return LockResponse.timedOut();
        }

        @Override
        public LockResponse visitUnknown(String unknownType) {
            throw new SafeIllegalStateException("Unknown response type", SafeArg.of("type", unknownType));
        }
    }

    static class RemoteTimeoutRetryer {
        private final Clock clock;

        @VisibleForTesting
        RemoteTimeoutRetryer(Clock clock) {
            this.clock = clock;
        }

        static BlockEnforcingLockService.RemoteTimeoutRetryer createDefault() {
            return new BlockEnforcingLockService.RemoteTimeoutRetryer(Clock.systemUTC());
        }

        <S, T> T attemptUntilTimeLimitOrException(
                S request,
                Duration duration,
                BiFunction<S, Duration, S> durationLimiter,
                Function<S, T> query,
                Predicate<T> isTimedOutResponse) {
            Instant now = clock.instant();
            Instant deadline = now.plus(duration);
            S currentRequest = request;
            T currentResponse = null;

            while (!now.isAfter(deadline)) {
                Duration remainingTime = Duration.between(now, deadline);
                currentRequest = durationLimiter.apply(currentRequest, remainingTime);

                try {
                    currentResponse = query.apply(currentRequest);
                    if (!isTimedOutResponse.test(currentResponse)) {
                        return currentResponse;
                    }
                    now = clock.instant();
                } catch (RuntimeException e) {
                    now = clock.instant();
                    if (!isPlausiblyTimeout(e) || now.isAfter(deadline)) {
                        throw e;
                    }
                }
            }

            Preconditions.checkNotNull(currentResponse, "We attempted to return because a blocking duration was"
                    + " reached. However, we had never tried applying the query. This indicates a bug in user requests"
                    + " or AtlasDB code.");
            return currentResponse;
        }

        private static boolean isPlausiblyTimeout(Throwable throwable) {
            return throwable instanceof SocketTimeoutException
                    || (throwable.getCause() != null && isPlausiblyTimeout(throwable.getCause()));
        }
    }
}
