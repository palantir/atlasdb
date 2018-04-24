/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.lock.v2;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.LockDescriptor;

@Value.Immutable
@JsonSerialize(as = ImmutableLockRequest.class)
@JsonDeserialize(as = ImmutableLockRequest.class)
public interface LockRequest {

    @Value.Parameter
    UUID getRequestId();

    @Value.Parameter
    Set<LockDescriptor> getLockDescriptors();

    @Value.Parameter
    long getAcquireTimeoutMs();

    @Value.Parameter
    Optional<String> getClientDescription();

    static LockRequest of(Set<LockDescriptor> lockDescriptors, long acquireTimeoutMs) {
        return ImmutableLockRequest.of(
                UUID.randomUUID(),
                lockDescriptors,
                acquireTimeoutMs,
                Optional.of("Thread: " + Thread.currentThread().getName()));
    }

    static LockRequest of(Set<LockDescriptor> lockDescriptors, long acquireTimeoutMs, String clientDescription) {
        return ImmutableLockRequest.of(
                UUID.randomUUID(),
                lockDescriptors,
                acquireTimeoutMs,
                Optional.of(clientDescription));
    }

}
