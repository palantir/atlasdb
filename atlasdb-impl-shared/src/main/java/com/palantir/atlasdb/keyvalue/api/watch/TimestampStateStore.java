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
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Value;

@NotThreadSafe
final class TimestampStateStore {
    private static final SafeLogger log = SafeLoggerFactory.get(TimestampStateStore.class);
    private static final int MAXIMUM_SIZE = 20_000;
    private final NavigableMap<StartTimestamp, MapEntry> timestampMap = new TreeMap<>();
    private final SortedSetMultimap<Sequence, StartTimestamp> livingVersions = TreeMultimap.create();

    void putStartTimestamps(Collection<Long> startTimestamps, LockWatchVersion version) {
        validateStateSize();

        startTimestamps.stream().map(StartTimestamp::of).forEach(startTimestamp -> {
            MapEntry previous = timestampMap.putIfAbsent(startTimestamp, MapEntry.of(version));
            Preconditions.checkArgument(previous == null, "Start timestamp already present in map");
            livingVersions.put(Sequence.of(version.version()), startTimestamp);
        });
    }

    void putCommitUpdates(Collection<TransactionUpdate> transactionUpdates, LockWatchVersion newVersion) {
        transactionUpdates.forEach(transactionUpdate -> {
            StartTimestamp startTimestamp = StartTimestamp.of(transactionUpdate.startTs());
            MapEntry previousEntry = timestampMap.get(startTimestamp);
            if (previousEntry == null) {
                throw new TransactionLockWatchFailedException("start timestamp missing from map");
            }

            Preconditions.checkArgument(
                    !previousEntry.commitInfo().isPresent(), "Commit info already present for given timestamp");

            timestampMap.replace(
                    startTimestamp,
                    previousEntry.withCommitInfo(CommitInfo.of(transactionUpdate.writesToken(), newVersion)));
        });
    }

    void remove(long startTimestamp) {
        Optional.ofNullable(timestampMap.remove(StartTimestamp.of(startTimestamp)))
                .ifPresent(entry -> livingVersions.remove(
                        Sequence.of(entry.version().version()), StartTimestamp.of(startTimestamp)));
    }

    void clear() {
        timestampMap.clear();
        livingVersions.clear();
    }

    Optional<LockWatchVersion> getStartVersion(long startTimestamp) {
        return Optional.ofNullable(timestampMap.get(StartTimestamp.of(startTimestamp)))
                .map(MapEntry::version);
    }

    Optional<CommitInfo> getCommitInfo(long startTimestamp) {
        return Optional.ofNullable(timestampMap.get(StartTimestamp.of(startTimestamp)))
                .flatMap(MapEntry::commitInfo);
    }

    @VisibleForTesting
    TimestampStateStoreState getStateForTesting() {
        return ImmutableTimestampStateStoreState.builder()
                .timestampMap(timestampMap)
                .livingVersions(livingVersions)
                .build();
    }

    Optional<Sequence> getEarliestLiveSequence() {
        return Optional.ofNullable(Iterables.getFirst(livingVersions.keySet(), null));
    }

    private void validateStateSize() {
        if (timestampMap.size() > MAXIMUM_SIZE || livingVersions.size() > MAXIMUM_SIZE) {
            log.warn(
                    "Timestamp state store has exceeded its maximum size. This likely indicates a memory leak",
                    SafeArg.of("timestampMapSize", timestampMap.size()),
                    SafeArg.of("livingVersionsSize", livingVersions.size()),
                    SafeArg.of("maximumSize", MAXIMUM_SIZE));
            throw new SafeIllegalStateException("Exceeded maximum timestamp state store size");
        }
    }

    @Value.Immutable
    @JsonDeserialize(as = ImmutableMapEntry.class)
    @JsonSerialize(as = ImmutableMapEntry.class)
    interface MapEntry {
        @Value.Parameter
        LockWatchVersion version();

        @Value.Parameter
        Optional<CommitInfo> commitInfo();

        static MapEntry of(LockWatchVersion version) {
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
        LockWatchVersion commitVersion();

        static CommitInfo of(LockToken commitLockToken, LockWatchVersion commitVersion) {
            return ImmutableCommitInfo.of(commitLockToken, commitVersion);
        }
    }
}
