/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

/**
 * Note: This class does NOT strictly guarantee that state returned is up to date across all requests.
 */
public class LockServiceStateDebugger {
    private final Map<LockClient, Set<LockRequest>> outstandingLockRequests;
    private final Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap;

    public LockServiceStateDebugger(
            Map<LockClient, Set<LockRequest>> outstandingLockRequests,
            Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap) {
        this.outstandingLockRequests = outstandingLockRequests;
        this.descriptorToLockMap = descriptorToLockMap;
    }

    public Multimap<LockClient, LockRequestProgress> getSuspectedLockProgress() {
        Multimap<LockClient, LockRequestProgress> result =
                Multimaps.newListMultimap(new HashMap<>(), Lists::newArrayList);
        for (Map.Entry<LockClient, Set<LockRequest>> entry : outstandingLockRequests.entrySet()) {
            for (LockRequest lockRequest : entry.getValue()) {
                result.put(entry.getKey(), getSuspectedLockProgress(entry.getKey(), lockRequest));
            }
        }
        return result;
    }

    private LockRequestProgress getSuspectedLockProgress(LockClient client, LockRequest request) {
        int locksHeld = 0;
        for (Map.Entry<LockDescriptor, LockMode> entry :
                request.getLockDescriptors().entries()) {
            ClientAwareReadWriteLock clientAwareLock = descriptorToLockMap.get(entry.getKey());
            KnownClientLock knownClientLock = clientAwareLock.get(client, entry.getValue());
            if (!knownClientLock.isHeld()) {
                return ImmutableLockRequestProgress.builder()
                        .request(request)
                        .nextLock(entry.getKey())
                        .numLocksAcquired(locksHeld)
                        .totalNumLocks(request.getLockDescriptors().size())
                        .build();
            }
            locksHeld++;
        }
        // This means that it looks like we hold ALL of the locks
        return ImmutableLockRequestProgress.builder()
                .request(request)
                .numLocksAcquired(locksHeld)
                .totalNumLocks(request.getLockDescriptors().size())
                .build();
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableLockRequestProgress.class)
    @JsonDeserialize(as = ImmutableLockRequestProgress.class)
    public interface LockRequestProgress {
        LockRequest getRequest();

        Optional<LockDescriptor> getNextLock();

        int getNumLocksAcquired();

        int getTotalNumLocks();
    }
}
