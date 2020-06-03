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

package com.palantir.lock.watch;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.immutables.value.Value;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.palantir.lock.v2.LockToken;

final class TimestampToVersionMap {
    private final Map<Long, MapEntry> timestampMap = new HashMap<>();
    private final TreeMultimap<IdentifiedVersion, Long> aliveVersions =
            TreeMultimap.create(IdentifiedVersion.comparator(), Ordering.natural());

    void put(long startTimestamp, IdentifiedVersion version) {
        timestampMap.put(startTimestamp, MapEntry.of(version));
        aliveVersions.put(version, startTimestamp);
    }

    boolean replace(TransactionUpdate transactionUpdate, IdentifiedVersion newVersion) {
        MapEntry previousEntry = timestampMap.get(transactionUpdate.startTs());
        if (previousEntry == null) {
            return false;
        }

        timestampMap.replace(transactionUpdate.startTs(), previousEntry.withCommitInfo(
                CommitInfo.of(transactionUpdate.commitTs(), transactionUpdate.writesToken(), newVersion)));

        return true;
    }

    void remove(long startTimestamp) {
        Optional.ofNullable(timestampMap.remove(startTimestamp))
                .ifPresent(entry -> aliveVersions.remove(entry.version(), startTimestamp));
    }

    void clear() {
        timestampMap.clear();
        aliveVersions.clear();
    }

    Optional<IdentifiedVersion> getEarliestVersion() {
        if (aliveVersions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(aliveVersions.keySet().first());
        }
    }

    Optional<MapEntry> get(long startTimestamp) {
        return Optional.ofNullable(timestampMap.get(startTimestamp));
    }

    @Value.Immutable
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
    interface CommitInfo {
        @Value.Parameter
        long commitTs();

        @Value.Parameter
        LockToken commitLockToken();

        @Value.Parameter
        IdentifiedVersion commitVersion();

        static CommitInfo of(long commitTs, LockToken commitLockToken, IdentifiedVersion commitVersion) {
            return ImmutableCommitInfo.of(commitTs, commitLockToken, commitVersion);
        }
    }

}
