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

import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.lock.v2.ImmutableLockRequest;
import com.palantir.lock.v2.ImmutableWaitForLocksRequest;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

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
        return timeoutRetryer.attemptUntilTimeLimitOrException(
                request,
                req -> Duration.ofMillis(req.getAcquireTimeoutMs()),
                BlockEnforcingLockService::clampLockRequestToDeadline,
                this::performSingleLockRequest,
                LockResponse::wasSuccessful,
                LockResponse.timedOut());
    }

    WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return timeoutRetryer.attemptUntilTimeLimitOrException(
                request,
                req -> Duration.ofMillis(req.getAcquireTimeoutMs()),
                BlockEnforcingLockService::clampWaitForLocksRequestToDeadline,
                this::performSingleWaitForLocksRequest,
                WaitForLocksResponse::wasSuccessful,
                WaitForLocksResponse.timedOut());
    }

    private static LockRequest clampLockRequestToDeadline(LockRequest request, Duration remainingTime) {
        return ImmutableLockRequest.builder()
                .from(request)
                .acquireTimeoutMs(remainingTime.toMillis())
                .build();
    }

    private static WaitForLocksRequest clampWaitForLocksRequestToDeadline(
            WaitForLocksRequest request, Duration remainingTime) {
        return ImmutableWaitForLocksRequest.builder()
                .from(request)
                .acquireTimeoutMs(remainingTime.toMillis())
                .build();
    }

    private LockResponse performSingleLockRequest(LockRequest request) {
        return namespacedConjureTimelockService
                .lock(ConjureLockRequests.toConjure(request))
                .accept(ToLeasedLockResponse.INSTANCE);
    }

    private WaitForLocksResponse performSingleWaitForLocksRequest(WaitForLocksRequest request) {
        return ConjureLockRequests.fromConjure(
                namespacedConjureTimelockService.waitForLocks(ConjureLockRequests.toConjure(request)));
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
