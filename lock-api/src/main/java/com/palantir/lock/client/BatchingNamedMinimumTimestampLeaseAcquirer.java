/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.AcquireNamedMinimumTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.lock.v2.LockNamedTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.timestamp.TimestampRange;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.immutables.value.Value;

public final class BatchingNamedMinimumTimestampLeaseAcquirer implements NamedMinimumTimestampLeaseAcquirer {
    private final DisruptorAutobatcher<Request, AcquireNamedMinimumTimestampLeaseResponse> autobatcher;

    private BatchingNamedMinimumTimestampLeaseAcquirer(
            DisruptorAutobatcher<Request, AcquireNamedMinimumTimestampLeaseResponse> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static BatchingNamedMinimumTimestampLeaseAcquirer create(LockLeaseService delegate) {
        DisruptorAutobatcher<Request, AcquireNamedMinimumTimestampLeaseResponse> autobatcher = Autobatchers.independent(
                        new AcquireNamedMinimumTimestampLeaseConsumer(delegate))
                .safeLoggablePurpose("single-client-acquire-named-timestamp-lease")
                .batchFunctionTimeout(Duration.ofSeconds(30))
                .build();
        return new BatchingNamedMinimumTimestampLeaseAcquirer(autobatcher);
    }

    @Override
    public LockNamedTimestampResponse lockNamedTimestamp(String timestampName, int numFreshTimestamps) {
        AcquireNamedMinimumTimestampLeaseResponse response = AtlasFutures.getUnchecked(autobatcher.apply(new Request() {
            @Override
            public String timestampName() {
                return timestampName;
            }

            @Override
            public int numFreshTimestamps() {
                return numFreshTimestamps;
            }
        }));

        return new LockNamedTimestampResponse() {
            @Override
            public long getMinimumLockedTimestamp() {
                return response.getMinimumLeasedTimestamp();
            }

            @Override
            public LockToken getLock() {
                return response.getLeaseToken();
            }

            @Override
            public TimestampRange getFreshTimestamps() {
                ConjureTimestampRange range = response.getFreshTimestamps();
                return TimestampRange.createRangeFromDeltaEncoding(range.getStart(), range.getCount());
            }
        };
    }

    @Override
    public void close() {}

    private static final class AcquireNamedMinimumTimestampLeaseConsumer
            implements Consumer<List<BatchElement<Request, AcquireNamedMinimumTimestampLeaseResponse>>> {
        private final LockLeaseService lockLeaseService;

        private AcquireNamedMinimumTimestampLeaseConsumer(LockLeaseService lockLeaseService) {
            this.lockLeaseService = lockLeaseService;
        }

        @Override
        public void accept(List<BatchElement<Request, AcquireNamedMinimumTimestampLeaseResponse>> batchElements) {
            Map<String, Long> timestampsToGetPerLessor = new HashMap<>();
            for (BatchElement<Request, AcquireNamedMinimumTimestampLeaseResponse> batchElement : batchElements) {
                String key = batchElement.argument().timestampName();
                long current = timestampsToGetPerLessor.getOrDefault(key, 0L);
                timestampsToGetPerLessor.put(key, current + batchElement.argument().numFreshTimestamps());
            }

            Map<String, LockNamedTimestampResponse>
            for (Map.Entry<String, Long> entry : timestampsToGetPerLessor.entrySet()) {
                lockLeaseService.lockNamedTimestamp(entry.getKey(), entry.getValue());
            }
        }
    }

    @Value.Immutable
    interface Request {
        String timestampName();

        int numFreshTimestamps();
    }
}
