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

import com.palantir.atlasdb.timelock.api.AcquireNamedMinimumTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.v2.LockNamedTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.timestamp.TimestampRange;

public final class NamespacedNamedMinimumTimestampLeaseAcquirer implements NamedMinimumTimestampLeaseAcquirer {
    private final Namespace namespace;
    private final ReferenceTrackingWrapper<MultiClientNamedMinimumTimestampLeaseAcquirer> referenceTrackingBatcher;

    NamespacedNamedMinimumTimestampLeaseAcquirer(
            Namespace namespace,
            ReferenceTrackingWrapper<MultiClientNamedMinimumTimestampLeaseAcquirer> referenceTrackingBatcher) {
        this.namespace = namespace;
        this.referenceTrackingBatcher = referenceTrackingBatcher;
    }

    @Override
    public LockNamedTimestampResponse lockNamedTimestamp(String timestampName, int numFreshTimestamps) {
        AcquireNamedMinimumTimestampLeaseResponse response = referenceTrackingBatcher
                .getDelegate()
                .acquireNamedTimestampLease(namespace, timestampName, numFreshTimestamps);

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
                ConjureTimestampRange deltaEncodingRange = response.getFreshTimestamps();
                return TimestampRange.createRangeFromDeltaEncoding(
                        deltaEncodingRange.getStart(), deltaEncodingRange.getCount());
            }
        };
    }

    @Override
    public void close() {
        referenceTrackingBatcher.close();
    }
}
