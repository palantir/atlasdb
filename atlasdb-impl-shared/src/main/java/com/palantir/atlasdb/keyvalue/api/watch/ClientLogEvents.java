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

import org.immutables.value.Value;

import com.google.common.collect.Range;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.logsafe.Preconditions;

@Value.Immutable
interface ClientLogEvents {

    LockWatchEvents events();

    boolean clearCache();

    @Value.Derived
    default Range<Long> eventsBySequence() {
        for (int i = 0; i < events().size() - 1; ++i) {
            Preconditions.checkArgument(events().get(i).sequence() + 1 == events().get(i + 1).sequence(),
                    "Events form a non-contiguous sequence");
        }


        long sequenceId = -1;
        events().forEach(event -> {

        });

        return Range.closed(1L, 2L);
    }

    default boolean containsVersion(IdentifiedVersion identifiedVersion) {
        eventsBySequence().contains(identifiedVersion.version());
    }

    @Value.Check
    default void checkContiguous() {
    }

    class Builder extends ImmutableClientLogEvents.Builder {}
}
