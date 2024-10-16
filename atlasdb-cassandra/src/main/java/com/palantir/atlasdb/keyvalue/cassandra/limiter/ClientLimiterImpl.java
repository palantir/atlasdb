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

package com.palantir.atlasdb.keyvalue.cassandra.limiter;

import com.palantir.atlasdb.keyvalue.cassandra.limiter.SafeSemaphore.CloseablePermit;
import com.palantir.atlasdb.util.MetricsManager;
import java.io.IOException;
import java.util.Map;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.thrift.TException;

public class ClientLimiterImpl implements ClientLimiter {

    private final SafeSemaphore concurrentRangeScans;

    public ClientLimiterImpl(int maxConcurrentRangeScans, MetricsManager metricsManager) {
        concurrentRangeScans = SafeSemaphore.of(maxConcurrentRangeScans);

        metricsManager.registerOrGet(
                ClientLimiter.class,
                "concurrent-range-scans",
                () -> maxConcurrentRangeScans - concurrentRangeScans.availablePermits(),
                Map.of());
    }

    @Override
    public <T> T limitGetRangeSlices(ThriftExceptionThrowingSupplier<T> closure) throws TException {
        try (CloseablePermit permit = concurrentRangeScans.tryAcquire().orElseThrow(TimedOutException::new)) {
            return closure.get();
        } catch (IOException e) {
            throw new TException(e);
        }
    }
}
