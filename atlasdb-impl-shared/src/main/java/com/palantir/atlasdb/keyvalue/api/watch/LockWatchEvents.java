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

import com.google.common.collect.Streams;
import com.palantir.lock.watch.LockWatchEvent;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public interface LockWatchEvents {
    List<LockWatchEvent> events();

    Optional<Long> latestSequence();

    static LockWatchEvents create(Set<Map.Entry<Long, LockWatchEvent>> versionToEventSet) {
        if (versionToEventSet.isEmpty()) {
            return ImmutableLockWatchEvents.builder().build();
        } else {
            return ImmutableLockWatchEvents.builder()
                    .addAllEvents(versionToEventSet.stream().map(Map.Entry::getValue).collect(Collectors.toList()))
                    .latestSequence(Streams.findLast(versionToEventSet.stream()).map(Map.Entry::getKey))
                    .build();
        }
    }

}
