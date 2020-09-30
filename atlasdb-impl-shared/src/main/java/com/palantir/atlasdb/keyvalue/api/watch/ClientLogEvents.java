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

package com.palantir.atlasdb.keyvalue.api.watch;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.immutables.value.Value;

import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.palantir.lock.watch.ImmutableTransactionsLockWatchUpdate;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.logsafe.Preconditions;

@Value.Immutable
interface ClientLogEvents {

    List<LockWatchEvent> events();

    boolean clearCache();

    @Value.Derived
    default Optional<Range<Long>> versionRange() {
        if (events().isEmpty()) {
            return Optional.empty();
        } else {
            LockWatchEvent firstEvent = Iterables.getFirst(events(), null);
            Preconditions.checkNotNull(firstEvent, "Should never try to compute version range from empty list");
            LockWatchEvent lastEvent = Iterables.getLast(events());
            return Optional.of(Range.closed(firstEvent.sequence(), lastEvent.sequence()));
        }
    }

    default TransactionsLockWatchUpdate map(Map<Long, LockWatchVersion> timestampMap) {
        return ImmutableTransactionsLockWatchUpdate.builder()
                .startTsToSequence(timestampMap)
                .events(events())
                .clearCache(clearCache())
                .build();
    }

    class Builder extends ImmutableClientLogEvents.Builder {}
}
