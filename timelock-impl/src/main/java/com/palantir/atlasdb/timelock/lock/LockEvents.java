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

package com.palantir.atlasdb.timelock.lock;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.WaitForLocksRequest;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import org.immutables.value.Value;

public interface LockEvents {
    void registerRequest(RequestInfo request);
    void timedOut(RequestInfo request, long acquisitionTimeMillis);
    void successfulAcquisition(RequestInfo request, long acquisitionTimeMillis);
    void lockExpired(UUID requestId, Collection<LockDescriptor> lockDescriptors);
    void explicitlyUnlocked(UUID requestId);

    @Value.Immutable
    interface RequestInfo {

        String EMPTY_DESCRIPTION = "<no description provided>";

        @Value.Parameter
        UUID id();

        @Value.Parameter
        String clientDescription();

        @Value.Parameter
        Set<LockDescriptor> lockDescriptors();

        static RequestInfo of(IdentifiedLockRequest request) {
            return ImmutableRequestInfo.of(
                    request.getRequestId(),
                    request.getClientDescription().orElse(EMPTY_DESCRIPTION),
                    request.getLockDescriptors());
        }

        static RequestInfo of(WaitForLocksRequest request) {
            return ImmutableRequestInfo.of(
                    request.getRequestId(),
                    request.getClientDescription().orElse(EMPTY_DESCRIPTION),
                    request.getLockDescriptors());
        }
    }
}
