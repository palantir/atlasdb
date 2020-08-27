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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.logsafe.Preconditions;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

final class TimestampStateStore {
    private final Map<Long, MapEntry> timestampMap = new HashMap<>();
    private final SortedSetMultimap<Long, Long> aliveVersions = TreeMultimap.create();

    void putStartTimestamps(Collection<Long> startTimestamps, IdentifiedVersion version) {
        startTimestamps.forEach(startTimestamp -> {
            MapEntry previous = timestampMap.putIfAbsent(startTimestamp, MapEntry.of(version));
            Preconditions.checkArgument(previous == null, "Start timestamp already present in map");
            aliveVersions.put(version.version(), startTimestamp);
        });
    }

    void putCommitUpdates(Collection<TransactionUpdate> transactionUpdates, IdentifiedVersion newVersion) {
        transactionUpdates.forEach(transactionUpdate -> {
            MapEntry previousEntry = timestampMap.get(transactionUpdate.startTs());
            if (previousEntry == null) {
                throw new TransactionLockWatchFailedException("start timestamp missing from map");
            }

            Preconditions.checkArgument(!previousEntry.commitInfo().isPresent(),
                    "Commit info already present for given timestamp");

            timestampMap.replace(
                    transactionUpdate.startTs(),
                    previousEntry.withCommitInfo(CommitInfo.of(transactionUpdate.writesToken(), newVersion)));
        });
    }

    void remove(long startTimestamp) {
        Optional.ofNullable(timestampMap.remove(startTimestamp))
                .ifPresent(entry -> aliveVersions.remove(entry.version().version(), startTimestamp));
    }

    void clear() {
        timestampMap.clear();
        aliveVersions.clear();
    }

    Optional<Long> getEarliestVersion() {
        return Optional.ofNullable(Iterables.getFirst(aliveVersions.keySet(), null));
    }

    Optional<IdentifiedVersion> getStartVersion(long startTimestamp) {
        return Optional.ofNullable(timestampMap.get(startTimestamp)).map(MapEntry::version);
    }

    Optional<CommitInfo> getCommitInfo(long startTimestamp) {
        return Optional.ofNullable(timestampMap.get(startTimestamp)).flatMap(MapEntry::commitInfo);
    }

    @VisibleForTesting
    TimestampStateStoreState getStateForTesting() {
        return ImmutableTimestampStateStoreState.builder()
                .timestampMap(timestampMap)
                .aliveVersions(aliveVersions)
                .build();
    }

    @Value.Immutable
    @JsonDeserialize(as = ImmutableMapEntry.class)
    @JsonSerialize(as = ImmutableMapEntry.class)
    interface MapEntry {
        @Value.Parameter
        IdentifiedVersion version();

        @Value.Parameter
        Optional<CommitInfo> commitInfo();

        static MapEntry of(IdentifiedVersion version) {
            return ImmutableMapEntry.of(version, Optional.empty());
        }

        default MapEntry withCommitInfo(CommitInfo commitInfo) {
            return ImmutableMapEntry.builder().from(this).commitInfo(commitInfo).build();
        }
    }

    @Value.Immutable
    @JsonDeserialize(as = ImmutableCommitInfo.class)
    @JsonSerialize(as = ImmutableCommitInfo.class)
    interface CommitInfo {
        @Value.Parameter
        LockToken commitLockToken();

        @Value.Parameter
        IdentifiedVersion commitVersion();

        static CommitInfo of(LockToken commitLockToken, IdentifiedVersion commitVersion) {
            return ImmutableCommitInfo.of(commitLockToken, commitVersion);
        }
    }

}
