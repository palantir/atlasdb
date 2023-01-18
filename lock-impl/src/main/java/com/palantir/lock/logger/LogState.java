/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.logger;

import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.Unsafe;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
@Unsafe
public interface LogState {
    @Safe
    List<SimpleLockRequestsWithSameDescriptor> getOutstandingRequests();

    @Safe
    Map<ObfuscatedLockDescriptor, SimpleTokenInfo> getHeldLocks();

    @Safe
    Map<ObfuscatedLockDescriptor, String> getSyncState();

    @Safe
    Map<String, List<SanitizedLockRequestProgress>> getSynthesizedRequestState();

    @Unsafe
    Map<ObfuscatedLockDescriptor, LockDescriptor> getLockDescriptorMapping();

    default void logTo(SafeLogger log) {
        log.info(
                "Lock server state",
                SafeArg.of("oustandingLockRequests", getOutstandingRequests()),
                SafeArg.of("heldLocks", getHeldLocks()),
                SafeArg.of("syncState", getSyncState()),
                SafeArg.of("synthesizedRequestState", getSynthesizedRequestState()),
                UnsafeArg.of("lockDescriptorMapping", getLockDescriptorMapping()));
    }
}
