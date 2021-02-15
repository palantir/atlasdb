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

import com.palantir.lock.watch.LockWatchVersion;
import java.util.Optional;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
interface VersionBounds {
    Optional<LockWatchVersion> startVersion();

    LockWatchVersion endVersion();

    /**
     * If the start version is too far behind and we need to take a snapshot, this field communicates how much we can
     * condense the events in that snapshot.
     */
    Optional<Long> earliestSnapshotVersion();

    @Value.Derived
    default long snapshotVersion() {
        return earliestSnapshotVersion().orElseGet(() -> endVersion().version());
    }

    @Value.Derived
    default UUID leader() {
        return endVersion().id();
    }

    class Builder extends ImmutableVersionBounds.Builder {}
}
