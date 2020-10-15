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
package com.palantir.lock.client;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableIdentifiedLockRequest.class)
@JsonDeserialize(as = ImmutableIdentifiedLockRequest.class)
public interface IdentifiedLockRequest {

    @Value.Parameter
    UUID getRequestId();

    @Value.Parameter
    Set<LockDescriptor> getLockDescriptors();

    @Value.Parameter
    long getAcquireTimeoutMs();

    @Value.Parameter
    Optional<String> getClientDescription();

    static IdentifiedLockRequest of(Set<LockDescriptor> lockDescriptors, long acquireTimeoutMs) {
        return ImmutableIdentifiedLockRequest.of(
                UUID.randomUUID(),
                lockDescriptors,
                acquireTimeoutMs,
                Optional.empty());
    }

    static IdentifiedLockRequest of(
            Set<LockDescriptor> lockDescriptors, long acquireTimeoutMs, String clientDescription) {
        return ImmutableIdentifiedLockRequest.of(
                UUID.randomUUID(),
                lockDescriptors,
                acquireTimeoutMs,
                Optional.of(clientDescription));
    }

    static IdentifiedLockRequest from(LockRequest lockRequest) {
        if (lockRequest.getClientDescription().isPresent()) {
            return of(
                    lockRequest.getLockDescriptors(),
                    lockRequest.getAcquireTimeoutMs(),
                    lockRequest.getClientDescription().get());
        }
        return of(lockRequest.getLockDescriptors(), lockRequest.getAcquireTimeoutMs());
    }

}
