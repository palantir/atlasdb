/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.debug;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import java.time.Instant;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.immutables.value.Value;

/**
 * TODO(jkong): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
public final class LocalLockTracker {
    private final Queue<TrackedLockEvent> eventBuffer;

    public LocalLockTracker(int sizeLimit) {
        eventBuffer = Queues.synchronizedQueue(EvictingQueue.create(sizeLimit));
    }

    List<TrackedLockEvent> getLocalLockHistory() {
        return ImmutableList.copyOf(eventBuffer);
    }

    void logLockResponse(Set<ConjureLockDescriptor> lockDescriptors, ConjureLockResponse response) {
        TrackedLockEvent event = getTimestampedLockEventBuilder()
                .eventType(EventType.LOCK)
                .eventDescription(response.accept(new ConjureLockResponse.Visitor<String>() {
                    @Override
                    public String visitSuccessful(SuccessfulLockResponse value) {
                        return "SUCCESS - locked " + lockDescriptors + "; obtained " + value;
                    }

                    @Override
                    public String visitUnsuccessful(UnsuccessfulLockResponse value) {
                        return "FAILED - tried to lock " + lockDescriptors;
                    }

                    @Override
                    public String visitUnknown(String unknownType) {
                        return "unexpected type: " + unknownType;
                    }
                })).build();
        eventBuffer.add(event);
    }

    void logWaitForLocksResponse(Set<ConjureLockDescriptor> lockDescriptors, ConjureWaitForLocksResponse response) {
        TrackedLockEvent event = getTimestampedLockEventBuilder()
                .eventType(EventType.WAIT_FOR_LOCKS)
                .eventDescription(response.getWasSuccessful()
                        ? "SUCCESS - waited for " + lockDescriptors
                        : "FAILED - tried to wait for " + lockDescriptors)
                .build();
        eventBuffer.add(event);
    }

    void logRefreshResponse(Set<ConjureLockToken> tokens, ConjureRefreshLocksResponse response) {
        TrackedLockEvent event = getTimestampedLockEventBuilder()
                .eventType(EventType.REFRESH_LOCKS)
                .eventDescription("Attempted to refresh " + tokens
                        + "; succeeded refreshing " + response.getRefreshedTokens())
                .build();
        eventBuffer.add(event);
    }

    void logUnlockResponse(Set<ConjureLockToken> tokens, ConjureUnlockResponse response) {
        TrackedLockEvent event = getTimestampedLockEventBuilder()
                .eventType(EventType.UNLOCK)
                .eventDescription("Attempted to unlock " + tokens
                        + "; succeeded unlocking " + response.getTokens())
                .build();
        eventBuffer.add(event);
    }

    private static ImmutableTrackedLockEvent.Builder getTimestampedLockEventBuilder() {
        return ImmutableTrackedLockEvent.builder().wallClockTimestamp(Instant.now());
    }

    enum EventType {
        LOCK,
        WAIT_FOR_LOCKS,
        REFRESH_LOCKS,
        UNLOCK
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableTrackedLockEvent.class)
    @JsonDeserialize(as = ImmutableTrackedLockEvent.class)
    interface TrackedLockEvent {
        Instant wallClockTimestamp();
        EventType eventType();
        String eventDescription();
    }
}
