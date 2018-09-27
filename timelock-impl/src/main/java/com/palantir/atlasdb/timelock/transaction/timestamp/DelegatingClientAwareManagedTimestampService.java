/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.transaction.timestamp;

import java.util.OptionalLong;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.timelock.paxos.AutoDelegate_ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.transaction.client.ModulusAllocator;
import com.palantir.atlasdb.timelock.transaction.client.NaiveModulusAllocator;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.TimestampRange;

public class DelegatingClientAwareManagedTimestampService
        implements AutoDelegate_ManagedTimestampService, ClientAwareManagedTimestampService {
    private static final Logger log = LoggerFactory.getLogger(DelegatingClientAwareManagedTimestampService.class);

    @VisibleForTesting
    static final int NUM_PARTITIONS = 16;

    private ModulusAllocator<UUID> allocator;
    private ManagedTimestampService delegate;

    @VisibleForTesting
    DelegatingClientAwareManagedTimestampService(
            ModulusAllocator<UUID> allocator,
            ManagedTimestampService delegate) {
        this.allocator = allocator;
        this.delegate = delegate;
    }

    public static ClientAwareManagedTimestampService createDefault(ManagedTimestampService delegate) {
        return new DelegatingClientAwareManagedTimestampService(
                new NaiveModulusAllocator<>(NUM_PARTITIONS), delegate);
    }

    @Override
    public long getFreshTimestampForClient(UUID clientIdentifier) {
        while (true) {
            TimestampRange timestampRange = delegate.getFreshTimestamps(NUM_PARTITIONS);
            int targetResidue = allocator.getRelevantModuli(clientIdentifier).iterator().next();
            OptionalLong relevantTimestamp = TimestampRanges.getTimestampMatchingModulus(
                    timestampRange,
                    targetResidue,
                    NUM_PARTITIONS);
            if (relevantTimestamp.isPresent()) {
                return relevantTimestamp.getAsLong();
            }

            // Not a bug - getFreshTimestamps is permitted to return less than the number of timestamps asked for,
            // so this case is possible.
            log.info("The timestamp range we received from the underlying timestamp service - {} -"
                            + " did not contain a value with residue {} modulo {}."
                            + " This is not a bug; but if it happens excessively frequently, please contact support.",
                    SafeArg.of("timestampRange", timestampRange),
                    SafeArg.of("targetResidue", targetResidue),
                    SafeArg.of("modulus", NUM_PARTITIONS));
        }
    }

    @Override
    public ManagedTimestampService delegate() {
        return delegate;
    }
}
