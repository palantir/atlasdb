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

package com.palantir.lock.client.timestampleases;

import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.lock.client.TimeLockUnlocker;
import com.palantir.lock.v2.TimestampLeaseResults;
import java.util.Map;

final class TimestampLeaseAcquirerImpl implements TimestampLeaseAcquirer {
    private final NamespacedTimestampLeaseService delegate;
    private final TimeLockUnlocker unlocker;

    public TimestampLeaseAcquirerImpl(NamespacedTimestampLeaseService delegate, TimeLockUnlocker unlocker) {
        this.delegate = delegate;
        this.unlocker = unlocker;
    }

    @Override
    public TimestampLeaseResults acquireNamedTimestampLeases(Map<TimestampLeaseName, Integer> requests) {
        // TODO(aalouane): implement
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void close() {
        // TODO(aalouane): decide whether or not to close the unlocker depending on ownership
    }
}
