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

import java.util.UUID;

import com.palantir.atlasdb.timelock.transaction.client.ModulusAllocator;
import com.palantir.atlasdb.timelock.transaction.client.NaiveClientModulusAllocator;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class DelegatingClientAwareTimestampService implements ClientAwareTimestampService {
    private static final int NUM_PARTITIONS = 16;

    private ModulusAllocator<UUID> allocator;
    private TimestampService delegate;

    private DelegatingClientAwareTimestampService(ModulusAllocator<UUID> allocator, TimestampService delegate) {
        this.allocator = allocator;
        this.delegate = delegate;
    }

    public static ClientAwareTimestampService createDefault(TimestampService delegate) {
        return new DelegatingClientAwareTimestampService(new NaiveClientModulusAllocator<>(16), delegate);
    }

    @Override
    public long getFreshTimestampForClient(UUID clientIdentifier) {
        TimestampRange timestampRange = delegate.getFreshTimestamps(NUM_PARTITIONS);
        return TimestampRanges.getTimestampMatchingModulus(
                timestampRange,
                allocator.getRelevantModuli(clientIdentifier).iterator().next(),
                NUM_PARTITIONS);
    }
}
