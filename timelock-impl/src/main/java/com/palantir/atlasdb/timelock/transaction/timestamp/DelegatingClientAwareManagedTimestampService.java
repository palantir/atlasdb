/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.transaction.timestamp;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.timelock.transaction.client.CachingPartitionAllocator;
import com.palantir.atlasdb.timelock.transaction.client.NumericPartitionAllocator;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.AutoDelegate_ManagedTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampRanges;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegatingClientAwareManagedTimestampService
        implements AutoDelegate_ManagedTimestampService, ClientAwareManagedTimestampService {
    private static final Logger log = LoggerFactory.getLogger(DelegatingClientAwareManagedTimestampService.class);

    @VisibleForTesting
    static final int NUM_PARTITIONS = TransactionConstants.V2_TRANSACTION_NUM_PARTITIONS;

    private NumericPartitionAllocator<UUID> allocator;
    private ManagedTimestampService delegate;

    @VisibleForTesting
    DelegatingClientAwareManagedTimestampService(
            NumericPartitionAllocator<UUID> allocator, ManagedTimestampService delegate) {
        this.allocator = allocator;
        this.delegate = delegate;
    }

    public static ClientAwareManagedTimestampService createDefault(ManagedTimestampService delegate) {
        NumericPartitionAllocator<UUID> allocator = CachingPartitionAllocator.createDefault(NUM_PARTITIONS);
        return new DelegatingClientAwareManagedTimestampService(allocator, delegate);
    }

    @Override
    public PartitionedTimestamps getFreshTimestampsForClient(UUID clientIdentifier, int numTransactionsRequested) {
        while (true) {
            TimestampRange timestampRange = delegate.getFreshTimestamps(NUM_PARTITIONS * numTransactionsRequested);
            int targetResidue =
                    allocator.getRelevantModuli(clientIdentifier).iterator().next();
            PartitionedTimestamps partitionedTimestamps =
                    TimestampRanges.getPartitionedTimestamps(timestampRange, targetResidue, NUM_PARTITIONS);

            if (partitionedTimestamps.count() > 0) {
                return partitionedTimestamps;
            }

            // Not a bug - getFreshTimestamps is permitted to return less than the number of timestamps asked for,
            // so this case is possible.
            log.info(
                    "The timestamp range we received from the underlying timestamp service - {} -"
                            + " did not contain a value with residue {} modulo {}. We will try again."
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
