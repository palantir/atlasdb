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

import java.util.Optional;

import com.palantir.lock.watch.CacheStatus;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchStateUpdate;

interface LockWatchEventLog {
    /**
     * @param lastKnownVersion latest version that the client knows about; should be before timestamps in the mapping;
     * @param endVersion       mapping from timestamp to identified version from client-side event cache;
     * @return lock watch events that occurred from (exclusive) the provided version, up to (inclusive) the latest
     * version in the timestamp to version map.
     */
    ClientLogEvents getEventsBetweenVersions(Optional<IdentifiedVersion> lastKnownVersion,
            IdentifiedVersion endVersion);

    Optional<IdentifiedVersion> getLatestKnownVersion();

    CacheStatus processUpdate(LockWatchStateUpdate update);

    void removeEventsBefore(long earliestSequence);
}
