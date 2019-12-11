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

package com.palantir.lock.watch;

import java.util.Set;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;

@Value.Immutable
public interface TimestampWithLockInfo {
    @Value.Parameter
    long timestamp();
    @Value.Parameter
    boolean invalidateAll();
    @Value.Parameter
    Set<LockDescriptor> locksSinceLastKnownState();

    static TimestampWithLockInfo invalidate(long timestamp) {
        return ImmutableTimestampWithLockInfo.of(timestamp, true, ImmutableSet.of());
    }

    static TimestampWithLockInfo diff(long timestamp, Set<LockDescriptor> newLocks) {
        return ImmutableTimestampWithLockInfo.of(timestamp, false, newLocks);
    }

    static TimestampWithLockInfo withNoLockInfo(long timestamp) {
        return diff(timestamp, ImmutableSet.of());
    }
}
