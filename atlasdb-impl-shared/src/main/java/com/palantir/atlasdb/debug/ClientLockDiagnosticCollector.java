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
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.LongStream;
import org.immutables.value.Value;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
public interface ClientLockDiagnosticCollector extends ConflictTracer {
    void collect(LongStream startTimestamps, long immutableTimestamp, UUID requestId);
    void collect(long startTimestamp, UUID requestId, Set<ConjureLockDescriptor> lockDescriptors);
    Map<Long, ClientLockDiagnosticDigest> getSnapshot();

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    @Value.Immutable
    @JsonDeserialize(as = ImmutableClientLockDiagnosticDigest.class)
    @JsonSerialize(as = ImmutableClientLockDiagnosticDigest.class)
    interface ClientLockDiagnosticDigest {
        long immutableTimestamp();
        UUID immutableTimestampRequestId();
        Map<UUID, Set<ConjureLockDescriptor>> lockRequests();
        List<ConflictTrace> writeWriteConflictTrace();

        static ClientLockDiagnosticDigest newTransaction(long immutableTimestamp, UUID immutableTimestampRequestId) {
            return ImmutableClientLockDiagnosticDigest.builder()
                    .immutableTimestamp(immutableTimestamp)
                    .immutableTimestampRequestId(immutableTimestampRequestId)
                    .build();
        }

        static ClientLockDiagnosticDigest newFragment() {
            return newTransaction(-2, UUID.nameUUIDFromBytes("fragment".getBytes(StandardCharsets.UTF_8)));
        }

        static ClientLockDiagnosticDigest missingEntry() {
            return newTransaction(-3, UUID.nameUUIDFromBytes("missingEntry".getBytes(StandardCharsets.UTF_8)));
        }

        default ClientLockDiagnosticDigest withLocks(UUID requestId, Set<ConjureLockDescriptor> lockDescriptors) {
            return ImmutableClientLockDiagnosticDigest.builder()
                    .from(this)
                    .putLockRequests(requestId, lockDescriptors)
                    .build();
        }

        default ClientLockDiagnosticDigest withNewConflictDigest(ConflictTrace conflictTrace) {
            return ImmutableClientLockDiagnosticDigest.builder()
                    .from(this)
                    .addWriteWriteConflictTrace(conflictTrace)
                    .build();
        }
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    @Value.Immutable
    @JsonDeserialize(as = ImmutableConflictTrace.class)
    @JsonSerialize(as = ImmutableConflictTrace.class)
    interface ConflictTrace {
        @Value.Parameter
        Map<Cell, Long> keysToLoad();

        @Value.Parameter
        Map<Cell, Long> latestTimestamps();

        @Value.Parameter
        Map<Long, Long> commitTimestamps();
    }
}
