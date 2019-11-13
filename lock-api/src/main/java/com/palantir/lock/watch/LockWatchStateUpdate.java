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

import java.util.Map;
import java.util.UUID;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import com.palantir.lock.LockDescriptor;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableLockWatchState.class)
@JsonDeserialize(as = ImmutableLockWatchState.class)
public interface LockWatchStateUpdate {
    LockWatchStateUpdate EMPTY = LockWatchStateUpdate.of(0L, UUID.randomUUID(), true, ImmutableMap.of());

    UUID leaderId();
    long version();
    boolean invalidateOldWatches();
    Map<LockDescriptor, LockWatchInfo> watchesUpdate();

    static LockWatchStateUpdate of(long version, UUID leaderId, boolean invalidateOldWatches,
            Map<LockDescriptor, LockWatchInfo> watchesUpdate) {
        return ImmutableLockWatchStateUpdate.builder()
                .version(version)
                .leaderId(leaderId)
                .invalidateOldWatches(invalidateOldWatches)
                .watchesUpdate(watchesUpdate)
                .build();
    }
}
