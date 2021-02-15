/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.debug;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.debug.ClientLockDiagnosticCollector.ClientLockDiagnosticDigest;
import com.palantir.atlasdb.debug.ClientLockDiagnosticCollector.ConflictTrace;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.immutables.value.Value;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
@Value.Immutable
@JsonDeserialize(as = ImmutableFullDiagnosticDigest.class)
@JsonSerialize(as = ImmutableFullDiagnosticDigest.class)
public interface FullDiagnosticDigest<T> {

    Set<Long> inProgressTransactions();

    Set<UUID> lockRequestIdsEvictedMidLockRequest();

    List<CompletedTransactionDigest<T>> completedTransactionDigests();

    RawData<T> rawData();

    List<LocalLockTracker.TrackedLockEvent> trackedLockEvents();

    @Value.Immutable
    @JsonDeserialize(as = ImmutableRawData.class)
    @JsonSerialize(as = ImmutableRawData.class)
    interface RawData<T> {
        @Value.Parameter
        WritesDigest<T> writeDigest();

        @Value.Parameter
        Optional<LockDiagnosticInfo> timelockDiagnosticInfo();

        @Value.Parameter
        Map<Long, ClientLockDiagnosticDigest> clientSideDiagnosticInfo();
    }

    @JsonDeserialize(as = ImmutableCompletedTransactionDigest.class)
    @JsonSerialize(as = ImmutableCompletedTransactionDigest.class)
    @Value.Immutable
    interface CompletedTransactionDigest<T> {
        long startTimestamp();

        long commitTimestamp();

        long immutableTimestamp();

        UUID immutableTimestampLockRequestId();

        T value();

        Map<UUID, LockDigest> locks();

        List<ConflictTrace> conflictTrace();
    }

    @JsonDeserialize(as = ImmutableLockDigest.class)
    @JsonSerialize(as = ImmutableLockDigest.class)
    @Value.Immutable
    interface LockDigest {
        Map<LockState, Instant> lockStates();

        Set<ConjureLockDescriptor> lockDescriptors();
    }
}
