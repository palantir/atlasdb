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

import com.google.common.primitives.Ints;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.ImmutableWaitForLocksResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.LockWatchVersion;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public final class ConjureLockRequests {
    private ConjureLockRequests() {}

    public static ConjureLockRequest toConjure(LockRequest request) {
        return ConjureLockRequest.builder()
                .lockDescriptors(toConjure(request.getLockDescriptors()))
                .clientDescription(request.getClientDescription())
                .requestId(UUID.randomUUID())
                .acquireTimeoutMs(Ints.checkedCast(request.getAcquireTimeoutMs()))
                .build();
    }

    public static ConjureLockRequest toConjure(WaitForLocksRequest request) {
        return ConjureLockRequest.builder()
                .lockDescriptors(toConjure(request.getLockDescriptors()))
                .clientDescription(request.getClientDescription())
                .requestId(request.getRequestId())
                .acquireTimeoutMs(Ints.checkedCast(request.getAcquireTimeoutMs()))
                .build();
    }

    private static Set<ConjureLockDescriptor> toConjure(Set<LockDescriptor> lockDescriptors) {
        return lockDescriptors.stream()
                .map(LockDescriptor::getBytes)
                .map(Bytes::from)
                .map(ConjureLockDescriptor::of)
                .collect(Collectors.toSet());
    }

    public static WaitForLocksResponse fromConjure(ConjureWaitForLocksResponse response) {
        return ImmutableWaitForLocksResponse.of(response.getWasSuccessful());
    }

    static Optional<ConjureIdentifiedVersion> toConjure(Optional<LockWatchVersion> maybeVersion) {
        return maybeVersion.map(identifiedVersion -> ConjureIdentifiedVersion.builder()
                .id(identifiedVersion.id())
                .version(identifiedVersion.version())
                .build());
    }
}
