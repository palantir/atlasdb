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

import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.Preconditions;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableLockWatchStateUpdate.class)
@JsonDeserialize(as = ImmutableLockWatchStateUpdate.class)
public interface LockWatchStateUpdate {
    LockWatchStateUpdate EMPTY = LockWatchStateUpdate.failure(UUID.randomUUID(), OptionalLong.empty());

    UUID leaderId();
    boolean success();
    OptionalLong lastKnownVersion();
    List<LockWatchEvent> events();

    @Value.Check
    default void successHasLastKnownVersion() {
        Preconditions.checkState(!success() || lastKnownVersion().isPresent(), "Success must have a version.");
    }

    @Value.Check
    default void lastEventSequenceMatchesLastKnownVersion() {
        if (!events().isEmpty()) {
            Preconditions.checkState(lastKnownVersion().isPresent(),
                    "If events are present, last known version must be present as well.");
            Preconditions.checkState(events().get(events().size() - 1).sequence() == lastKnownVersion().getAsLong(),
                    "The sequence of the last event and the last known version must match");
        }
    }

    static LockWatchStateUpdate of(UUID leaderId, boolean success, OptionalLong version, List<LockWatchEvent> events) {
        return ImmutableLockWatchStateUpdate.builder()
                .leaderId(leaderId)
                .success(success)
                .lastKnownVersion(version)
                .events(events)
                .build();
    }

    static LockWatchStateUpdate failure(UUID uuid, OptionalLong lastKnownVersion) {
        return of(uuid, false, lastKnownVersion, ImmutableList.of());
    }

    static LockWatchStateUpdate update(UUID uuid, long lastKnownVersion, List<LockWatchEvent> events) {
        return of(uuid, true, OptionalLong.of(lastKnownVersion), events);
    }
}
