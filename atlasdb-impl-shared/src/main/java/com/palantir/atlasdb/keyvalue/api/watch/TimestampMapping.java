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

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.immutables.value.Value;

import com.google.common.collect.Range;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.logsafe.Preconditions;

@Value.Immutable
interface TimestampMapping {
    Map<Long, LockWatchVersion> timestampMapping();

    @Value.Derived
    default Range<Long> versionRange() {
        Optional<Long> firstVersion = timestampMapping()
                .values()
                .stream()
                .map(LockWatchVersion::version)
                .min(Long::compareTo);
        Optional<Long> lastVersion = timestampMapping()
                .values()
                .stream()
                .map(LockWatchVersion::version)
                .min(Long::compareTo);

        Preconditions.checkState(firstVersion.isPresent(),
                "Cannot compute timestamp mapping for empty map of timestamps");
        Preconditions.checkState(lastVersion.isPresent(),
                "Cannot compute timestamp mapping for empty map of timestamps");

        return Range.closed(firstVersion.get(), lastVersion.get());
    }

    @Value.Derived
    default UUID leader() {
        
    }

    class Builder extends ImmutableTimestampMapping.Builder {}
}
