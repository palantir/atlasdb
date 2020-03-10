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

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.lock.v2.ImmutableLockRequest;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * Splits a lock acquisition into segments up to a predefined interval. Ensures that each acquisition does not
 * block for more than the predefined interval.
 *
 * This is done to account for the reality of bounded timeouts (e.g. in terms of the networking layer) when
 * communicating with remote services.
 */
class AcquisitionSegmentingLockService {
    private final NamespacedConjureTimelockService namespacedConjureTimelockService;
    private final int maximumRequestBoundMillis;

    public AcquisitionSegmentingLockService(
            NamespacedConjureTimelockService namespacedConjureTimelockService,
            int maximumRequestBoundMillis) {
        Preconditions.checkState(maximumRequestBoundMillis > 0,
                "Maximum request bound millis must be positive");
        this.namespacedConjureTimelockService = namespacedConjureTimelockService;
        this.maximumRequestBoundMillis = maximumRequestBoundMillis;
    }

    LockResponse lock(LockRequest request) {
        if (hasTimeoutWithinBound(request)) {
            return performSingleRequest(request);
        }

        Instant now = Instant.now();
        Instant deadline = now.plus(Duration.ofMillis(request.getAcquireTimeoutMs()));
        while (now.isBefore(deadline)) {
            Duration remainingTime = Duration.between(now, deadline);
            LockResponse singleRequestResponse = performSingleRequest(clampRequestToBound(request, remainingTime));
            if (singleRequestResponse.wasSuccessful()) {
                return singleRequestResponse;
            }
            now = Instant.now();
        }
        return LockResponse.timedOut();
    }

    private LockResponse performSingleRequest(LockRequest request) {
        return namespacedConjureTimelockService
                .lock(ConjureLockRequests.toConjure(request))
                .accept(ToLeasedLockResponse.INSTANCE);
    }

    private LockRequest clampRequestToBound(LockRequest request, Duration remainingTime) {
        return ImmutableLockRequest.builder()
                .from(request)
                .acquireTimeoutMs(Math.min(maximumRequestBoundMillis, remainingTime.toMillis()))
                .build();
    }

    private boolean hasTimeoutWithinBound(LockRequest request) {
        return request.getAcquireTimeoutMs() <= maximumRequestBoundMillis;
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
}
