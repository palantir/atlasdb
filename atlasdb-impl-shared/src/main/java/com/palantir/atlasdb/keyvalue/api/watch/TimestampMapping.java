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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.collect.Range;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
interface TimestampMapping {
    Map<Long, LockWatchVersion> timestampMapping();

    @Value.Derived
    default Range<Long> versionRange() {
        LongSummaryStatistics summary = timestampMapping().values().stream()
                .mapToLong(LockWatchVersion::version)
                .summaryStatistics();
        return Range.closed(summary.getMin(), summary.getMax());
    }

    @Value.Derived
    default LockWatchVersion lastVersion() {
        return LockWatchVersion.of(leader(), versionRange().upperEndpoint());
    }

    @Value.Derived
    default UUID leader() {
        // explicitly avoiding stream and distinct collection copy when all IDs should be identical
        return identicalElement(Collections2.transform(timestampMapping().values(), LockWatchVersion::id));
    }

    @Value.Check
    default void nonEmptyMapping() {
        Preconditions.checkArgument(!timestampMapping().isEmpty(), "Cannot process an empty timestamp map");
    }

    static ImmutableTimestampMapping.Builder builder() {
        return ImmutableTimestampMapping.builder();
    }

    @VisibleForTesting
    static <T> T identicalElement(Iterable<? extends T> iterable) {
        Iterator<? extends T> iterator = iterable.iterator();
        T element = iterator.next();
        while (iterator.hasNext()) {
            T next = iterator.next();
            if (!Objects.equals(element, next)) {
                throw new SafeIllegalArgumentException(
                        "Input contains multiple distinct elements",
                        SafeArg.of("element", element),
                        SafeArg.of("next", next));
            }
        }
        return element;
    }
}
