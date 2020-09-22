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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.logsafe.Preconditions;

final class TimestampStateStore {
    private static final ImmutableTimestampVersion INITIAL_VERSION = ImmutableTimestampVersion.of(
            TransactionConstants.FAILED_COMMIT_TS, IdentifiedVersion.of(
                    UUID.randomUUID(), -1));

    private final Map<Long, MapEntry> timestampMap = new HashMap<>();
    private final SortedSetMultimap<Long, Long> aliveVersions = TreeMultimap.create();
    private ImmutableTimestampVersion lastImmutableTimestamp = INITIAL_VERSION;

    void putStartTimestamps(long immutableTimestamp, Collection<Long> startTimestamps, IdentifiedVersion version) {
        lastImmutableTimestamp = lastImmutableTimestamp.max(immutableTimestamp, version);
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
        lastImmutableTimestamp = INITIAL_VERSION;
    }

    Optional<Long> getEarliestVersion() {
        long immutableTimestampVersion = lastImmutableTimestamp.version();
        return Optional.ofNullable(Iterables.getFirst(aliveVersions.keySet(), null))
                .map(firstAliveVersion -> Math.min(immutableTimestampVersion, firstAliveVersion));
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

    @Value.Immutable
    @JsonDeserialize(as = ImmutableCommitInfo.class)
    @JsonSerialize(as = ImmutableCommitInfo.class)
    interface ImmutableTimestampVersion {
        @Value.Parameter
        long immutableTimestamp();

        @Value.Parameter
        long version();

        default ImmutableTimestampVersion max(long immutableTimestamp, IdentifiedVersion version) {
            if (immutableTimestamp() < immutableTimestamp) {
                return ImmutableTimestampVersion.of(immutableTimestamp, version);
            } else {
                return this;
            }
        }

        static ImmutableTimestampVersion of(long immutableTimestamp, IdentifiedVersion version) {
            return ImmutableImmutableTimestampVersion.of(immutableTimestamp, version.version());
        }
    }
}
