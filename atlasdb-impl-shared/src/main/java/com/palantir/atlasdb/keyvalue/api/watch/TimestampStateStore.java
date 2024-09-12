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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.Unsafe;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collection;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Value;

/**
 * Stores mappings of a timestamp to associated information, as well as tracks the earliest live version for the sake
 * of determining how far we can retention in the {@link VersionedEventStore}.
 *
 * Each timestamp has a mapping to its start version (as timestamps are added here at start transaction time, and thus
 * definitely have this information). Each timestamp may also be updated with its commit version and commit lock
 * token (in a single operation). These together may then be used to retrieve relevant events for that transaction.
 *
 * Note that this class may not be thread safe in the general case, but can be used safely, depending on the caller. The
 * main things to call out are:
 *
 * 1. Each timestamp is independent of each other, and thus updates should not interact. Updates to the same key may
 *    be executed in any order (and indeed, an update may race the initial put), but these should be handled by the
 *    caller.
 * 2. The entries in the living versions multimap may not be independent (as a single version may correspond to many
 *    timestamps), but the update concurrency should be handled by the data structure.
 * 3. Calls to {@link #getEarliestLiveSequence()} uses a snapshot of livingVersions map;
 *    this method should be called sparsely. Given that it is only used for retentioning events, which can be eventually
 *    consistent (as it is always correct to keep more events rather than less), this is acceptable for performance.
 */
@NotThreadSafe
final class TimestampStateStore {
    private static final SafeLogger log = SafeLoggerFactory.get(TimestampStateStore.class);

    // The purpose of this relates to concern about the timestamp state store using excessive memory.
    // Each mapping is associated with a long (start timestamp), three UUIDs and two longs maximum (timestamp map)
    // and a reverse mapping of two longs in livingVersions. So even in the largest case, we have ~100 bytes per
    // mapping, meaning that the timestamp cache size should not exceed ~10 MiB.
    @VisibleForTesting
    static final int MAXIMUM_SIZE = 100_000;

    private final NavigableMap<StartTimestamp, TimestampVersionInfo> timestampMap = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<Sequence, NavigableSet<StartTimestamp>> livingVersions = new ConcurrentHashMap<>();

    void putStartTimestamps(Collection<Long> startTimestamps, LockWatchVersion version) {
        if (startTimestamps.isEmpty()) {
            return;
        }

        validateStateSize();

        Sequence sequence = Sequence.of(version.version());
        TimestampVersionInfo currentVersion = TimestampVersionInfo.of(version);
        startTimestamps.stream().map(StartTimestamp::of).forEach(startTimestamp -> {
            TimestampVersionInfo previous = timestampMap.putIfAbsent(startTimestamp, currentVersion);
            Preconditions.checkArgument(previous == null, "Start timestamp already present in map");
            livingVersions
                    .computeIfAbsent(sequence, _seq -> new ConcurrentSkipListSet<>())
                    .add(startTimestamp);
        });
    }

    void putCommitUpdates(Collection<TransactionUpdate> transactionUpdates, LockWatchVersion newVersion) {
        transactionUpdates.forEach(transactionUpdate -> {
            StartTimestamp startTimestamp = StartTimestamp.of(transactionUpdate.startTs());
            TimestampVersionInfo previousEntry = timestampMap.get(startTimestamp);
            if (previousEntry == null) {
                throw new TransactionLockWatchFailedException("Start timestamp missing from map");
            }

            Preconditions.checkArgument(
                    previousEntry.commitInfo().isEmpty(), "Commit info already present for given timestamp");

            timestampMap.replace(
                    startTimestamp,
                    previousEntry.withCommitInfo(CommitInfo.of(transactionUpdate.writesToken(), newVersion)));
        });
    }

    void remove(long startTimestamp) {
        StartTimestamp startTs = StartTimestamp.of(startTimestamp);
        TimestampVersionInfo entry = timestampMap.remove(startTs);
        if (entry != null) {
            Sequence seq = Sequence.of(entry.version().version());
            NavigableSet<StartTimestamp> startTimestamps = livingVersions.get(seq);
            if (startTimestamps != null && startTimestamps.remove(startTs) && startTimestamps.isEmpty()) {
                // clean up if this was the last timestamp for sequence
                livingVersions.remove(seq, ImmutableSortedSet.of());
            }
        }
    }

    void clear() {
        timestampMap.clear();
        livingVersions.clear();
    }

    Optional<LockWatchVersion> getStartVersion(long startTimestamp) {
        return getTimestampInfo(startTimestamp).map(TimestampVersionInfo::version);
    }

    Optional<TimestampVersionInfo> getTimestampInfo(long startTimestamp) {
        return Optional.ofNullable(timestampMap.get(StartTimestamp.of(startTimestamp)));
    }

    Optional<Sequence> getEarliestLiveSequence() {
        return livingVersions.keySet().stream().min(Comparator.naturalOrder());
    }

    @VisibleForTesting
    Optional<CommitInfo> getCommitInfo(long startTimestamp) {
        return getTimestampInfo(startTimestamp).flatMap(TimestampVersionInfo::commitInfo);
    }

    @Unsafe
    TimestampStateStoreState getStateForDiagnostics() {
        // This method doesn't need to read a thread-safe snapshot of timestampMap and livingVersions
        SortedSetMultimap<Sequence, StartTimestamp> living = TreeMultimap.create();
        livingVersions.forEach(
                (sequence, startTimestamps) -> living.putAll(sequence, ImmutableSortedSet.copyOf(startTimestamps)));

        return ImmutableTimestampStateStoreState.builder()
                .timestampMap(ImmutableSortedMap.copyOf(timestampMap))
                .livingVersions(Multimaps.unmodifiableSortedSetMultimap(living))
                .build();
    }

    private void validateStateSize() {
        int timestampMapSize = timestampMap.size();
        int livingVersionsSize = livingVersions.size();
        if (timestampMapSize > MAXIMUM_SIZE || livingVersionsSize > MAXIMUM_SIZE) {
            log.warn(
                    "Timestamp state store has exceeded its maximum size. This likely indicates a memory leak",
                    SafeArg.of("timestampMapSize", timestampMapSize),
                    SafeArg.of("livingVersionsSize", livingVersionsSize),
                    SafeArg.of("maximumSize", MAXIMUM_SIZE),
                    SafeArg.of("minimumLiveTimestamp", timestampMap.firstEntry()),
                    SafeArg.of("maximumLiveTimestamp", timestampMap.lastEntry()),
                    SafeArg.of("minimumLiveVersion", getEarliestLiveSequence()));
            throw new SafeIllegalStateException("Exceeded maximum timestamp state store size");
        }
    }

    @Value.Immutable
    @JsonDeserialize(as = ImmutableTimestampVersionInfo.class)
    @JsonSerialize(as = ImmutableTimestampVersionInfo.class)
    interface TimestampVersionInfo {
        @Value.Parameter
        LockWatchVersion version();

        @Value.Parameter
        Optional<CommitInfo> commitInfo();

        static TimestampVersionInfo of(LockWatchVersion version) {
            return ImmutableTimestampVersionInfo.of(version, Optional.empty());
        }

        default TimestampVersionInfo withCommitInfo(CommitInfo commitInfo) {
            return ImmutableTimestampVersionInfo.builder()
                    .from(this)
                    .commitInfo(commitInfo)
                    .build();
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
