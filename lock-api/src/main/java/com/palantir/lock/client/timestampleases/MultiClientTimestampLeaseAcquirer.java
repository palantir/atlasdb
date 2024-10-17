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

import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.lock.v2.PartialTimestampLeaseResult;
import java.time.Duration;
import java.util.Map;

public final class MultiClientTimestampLeaseAcquirer implements AutoCloseable {
    private final DisruptorAutobatcher<
                    TimestampLeaseAcquirerBatchParams, Map<TimestampLeaseName, PartialTimestampLeaseResult>>
            autobatcher;

    private MultiClientTimestampLeaseAcquirer(
            DisruptorAutobatcher<
                            TimestampLeaseAcquirerBatchParams, Map<TimestampLeaseName, PartialTimestampLeaseResult>>
                    autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static MultiClientTimestampLeaseAcquirer create(InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<TimestampLeaseAcquirerBatchParams, Map<TimestampLeaseName, PartialTimestampLeaseResult>>
                autobatcher = Autobatchers.independent(new TimestampLeaseAcquirerBatchHandler(delegate))
                        .safeLoggablePurpose("timestamp-lease-acquier")
                        .batchFunctionTimeout(Duration.ofSeconds(30))
                        .build();

        return new MultiClientTimestampLeaseAcquirer(autobatcher);
    }

    public Map<TimestampLeaseName, PartialTimestampLeaseResult> acquireTimestampLeases(
            Namespace namespace, Map<TimestampLeaseName, Integer> requests) {
        return AtlasFutures.getUnchecked(autobatcher.apply(TimestampLeaseAcquirerBatchParams.of(namespace, requests)));
    }

    @Override
    public void close() {
        autobatcher.close();
    }
}
