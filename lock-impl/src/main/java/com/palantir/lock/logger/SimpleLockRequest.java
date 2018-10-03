/**
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.lock.logger;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.BlockingMode;
import com.palantir.lock.LockGroupBehavior;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.TimeDuration;

@JsonDeserialize(as = ImmutableSimpleLockRequest.class)
@JsonSerialize(as = ImmutableSimpleLockRequest.class)
@Value.Immutable
public abstract class SimpleLockRequest {

    public static SimpleLockRequest of(LockRequest request, String lockDescriptor, LockMode lockMode, String clientId) {
        return ImmutableSimpleLockRequest.builder()
                .lockDescriptor(lockDescriptor)
                .lockMode(lockMode)
                .lockCount(request.getLocks().size())
                .lockTimeout(request.getLockTimeout().toMillis())
                .lockGroupBehavior(request.getLockGroupBehavior())
                .blockingMode(request.getBlockingMode())
                .blockingDuration(extractBlockingDurationOrNull(request.getBlockingDuration()))
                .versionId(request.getVersionId())
                .creatingThread(request.getCreatingThreadName())
                .clientId(clientId).build();
    }

    @Value.Parameter
    public abstract String getLockDescriptor();

    @Value.Parameter
    public abstract LockMode getLockMode();

    @Value.Parameter
    public abstract long getLockCount();

    @Value.Parameter
    public abstract long getLockTimeout();

    @Value.Parameter
    public abstract LockGroupBehavior getLockGroupBehavior();

    @Value.Parameter
    public abstract BlockingMode getBlockingMode();

    @Nullable
    @Value.Parameter
    public abstract Long getBlockingDuration();

    @Nullable
    @Value.Parameter
    public abstract Long getVersionId();

    @Value.Parameter
    public abstract String getClientId();

    @Value.Parameter
    public abstract String getCreatingThread();

    @Nullable
    private static Long extractBlockingDurationOrNull(TimeDuration blockingDuration) {
        return  (blockingDuration != null) ? blockingDuration.toMillis() : null;
    }
}
