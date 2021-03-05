/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

import java.util.List;
import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonSerialize(as = ImmutableLockState.class)
@JsonDeserialize(as = ImmutableLockState.class)
public interface LockState {
    boolean isWriteLocked();
    boolean isFrozen();
    List<LockClient> exactCurrentLockHolders();
    List<LockHolder> holders();
    List<LockRequester> requesters();

    @Value.Immutable
    @JsonSerialize(as = ImmutableLockHolder.class)
    @JsonDeserialize(as = ImmutableLockHolder.class)
    interface LockHolder {
        LockClient client();
        long creationDateMs();
        long expirationDateMs();
        int numOtherLocksHeld();
        Optional<Long> versionId();
        String requestingThread();
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableLockRequester.class)
    @JsonDeserialize(as = ImmutableLockRequester.class)
    interface LockRequester {
        LockClient client();
        LockGroupBehavior lockGroupBehavior();
        BlockingMode blockingMode();
        Optional<TimeDuration> blockingDuration();
        Optional<Long> versionId();
        String requestingThread();
    }
}
