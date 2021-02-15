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

import com.google.common.collect.Range;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface LockWatchEvents {
    List<LockWatchEvent> events();

    @Value.Derived
    default Optional<Range<Long>> versionRange() {
        LongSummaryStatistics summary =
                events().stream().mapToLong(LockWatchEvent::sequence).summaryStatistics();

        if (summary.getCount() == 0) {
            return Optional.empty();
        } else {
            return Optional.of(Range.closed(summary.getMin(), summary.getMax()));
        }
    }

    @Value.Check
    default void contiguousSequence() {
        if (events().isEmpty()) {
            return;
        }

        for (int i = 0; i < events().size() - 1; ++i) {
            Preconditions.checkArgument(
                    events().get(i).sequence() + 1 == events().get(i + 1).sequence(),
                    "Events form a non-contiguous sequence");
        }
    }

    @Value.Check
    default void rangeOnlyPresentIffEventsAre() {
        if (events().isEmpty()) {
            Preconditions.checkState(!versionRange().isPresent(), "Cannot have a version range with no events");
        } else {
            Preconditions.checkState(versionRange().isPresent(), "Non-empty events must have a version range");
        }
    }

    default void assertNoEventsAreMissing(Optional<LockWatchVersion> latestVersion) {
        if (events().isEmpty()) {
            return;
        }

        if (latestVersion.isPresent()) {
            Preconditions.checkArgument(versionRange().isPresent(), "First element not preset in list of events");
            long firstVersion = versionRange().get().lowerEndpoint();
            Preconditions.checkArgument(
                    firstVersion <= latestVersion.get().version()
                            || latestVersion.get().version() + 1 == firstVersion,
                    "Events missing between last snapshot and this batch of events",
                    SafeArg.of("latestVersionSequence", latestVersion.get().version()),
                    SafeArg.of("firstNewVersionSequence", firstVersion));
        }
    }

    class Builder extends ImmutableLockWatchEvents.Builder {}
}
