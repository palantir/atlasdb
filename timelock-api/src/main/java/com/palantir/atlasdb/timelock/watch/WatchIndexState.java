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

package com.palantir.atlasdb.timelock.watch;

import java.util.Comparator;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.LockDescriptor;

@JsonSerialize(as = ImmutableWatchIndexState.class)
@JsonDeserialize(as = ImmutableWatchIndexState.class)
@Value.Immutable
public interface WatchIndexState extends Comparable<WatchIndexState> {
    Comparator<WatchIndexState> WATCH_INDEX_STATE_COMPARATOR
            = Comparator.comparing(WatchIndexState::lastUnlockSequence)
            .thenComparing(WatchIndexState::lastLockSequence);

    long lastLockSequence();
    long lastUnlockSequence();

    static WatchIndexState createDefaultForLockDescriptor(LockDescriptor descriptor) {
        return of(0, 0);
    }

    static WatchIndexState of(long lastLock, long lastUnlock) {
        return ImmutableWatchIndexState.builder()
                .lastLockSequence(lastLock)
                .lastUnlockSequence(lastUnlock)
                .build();
    }

    @Override
    default int compareTo(@Nonnull WatchIndexState other) {
        return WATCH_INDEX_STATE_COMPARATOR.compare(this, other);
    }
}
