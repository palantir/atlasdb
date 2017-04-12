/**
 * Copyright 2017 Palantir Technologies
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

import com.palantir.lock.LockRequest;
import com.palantir.lock.TimeDuration;

@Value.Immutable
public abstract class SimpleLockRequest {

    public static SimpleLockRequest of(LockRequest request, String lockDescriptor, String clientId) {
        return ImmutableSimpleLockRequest.builder()
                .lockDescriptor(lockDescriptor)
                .lockCount(request.getLocks().size())
                .lockTimeout(request.getLockTimeout().getTime())
                .lockGroupBehavior(request.getLockGroupBehavior().name())
                .blockingMode(request.getBlockingMode().name())
                .blockingDuration(extractBlockingDurationOrNull(request.getBlockingDuration()))
                .versionId(request.getVersionId())
                .creatingThread(request.getCreatingThreadName())
                .clientId(clientId).build();
    }

    @Value.Parameter
    public abstract String getLockDescriptor();

    @Value.Parameter
    public abstract long getLockCount();

    @Value.Parameter
    public abstract long getLockTimeout();

    @Value.Parameter
    public abstract String getLockGroupBehavior();

    @Value.Parameter
    public abstract String getBlockingMode();

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
        return  (blockingDuration != null) ? blockingDuration.getTime() : null;
    }
}