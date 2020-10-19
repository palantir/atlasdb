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
package com.palantir.lock.v2;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.Preconditions;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableWaitForLocksRequest.class)
@JsonDeserialize(as = ImmutableWaitForLocksRequest.class)
public interface WaitForLocksRequest {

    @Value.Parameter
    UUID getRequestId();

    @Value.Parameter
    Set<LockDescriptor> getLockDescriptors();

    @Value.Parameter
    long getAcquireTimeoutMs();

    @Value.Parameter
    Optional<String> getClientDescription();

    static WaitForLocksRequest of(Set<LockDescriptor> lockDescriptors, long acquireTimeoutMs) {
        return ImmutableWaitForLocksRequest.of(UUID.randomUUID(), lockDescriptors, acquireTimeoutMs, Optional.empty());
    }

    static WaitForLocksRequest of(Set<LockDescriptor> lockDescriptors, long acquireTimeoutMs,
            @Nullable String clientDescription) {
        return ImmutableWaitForLocksRequest.of(UUID.randomUUID(), lockDescriptors, acquireTimeoutMs,
                Optional.ofNullable(clientDescription));
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(getAcquireTimeoutMs() >= 0, "Acquire timeout cannot be negative.");
    }
}
