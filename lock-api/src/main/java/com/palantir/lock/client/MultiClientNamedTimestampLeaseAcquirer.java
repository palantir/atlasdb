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
import com.palantir.atlasdb.timelock.api.GenericNamedMinTimestamp;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.v2.PartialNamedTimestampLeaseResult;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.immutables.value.Value;

public final class MultiClientNamedTimestampLeaseAcquirer implements AutoCloseable {
    private final DisruptorAutobatcher<NamespaceAndRequests, Map<GenericNamedMinTimestamp, PartialNamedTimestampLeaseResult>> autobatcher;

    private MultiClientNamedTimestampLeaseAcquirer(
            DisruptorAutobatcher<NamespaceAndRequests, Map<GenericNamedMinTimestamp, PartialNamedTimestampLeaseResult>> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static MultiClientNamedTimestampLeaseAcquirer create(InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<NamespaceAndRequests, Map<GenericNamedMinTimestamp, PartialNamedTimestampLeaseResult>> autobatcher =
                Autobatchers.independent(consumer(delegate))
                        .safeLoggablePurpose("named-timestamp-lease-acquirer")
                        .batchFunctionTimeout(Duration.ofSeconds(30))
                        .build();

        return new MultiClientNamedTimestampLeaseAcquirer(autobatcher);
    }

    public Map<GenericNamedMinTimestamp, PartialNamedTimestampLeaseResult> acquireNamedTimestampLeased(
            Namespace namespace, Map<GenericNamedMinTimestamp, Integer> requests) {
        return AtlasFutures.getUnchecked(autobatcher.apply(NamespaceAndRequests.of(namespace, requests)));
    }

    private static Consumer<List<BatchElement<NamespaceAndRequests, Map<GenericNamedMinTimestamp, PartialNamedTimestampLeaseResult>>>>
            consumer(InternalMultiClientConjureTimelockService delegate) {
        return null;
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    @Value.Immutable
    interface NamespaceAndRequests {
        @Value.Parameter
        Namespace namespace();

        @Value.Parameter
        Map<GenericNamedMinTimestamp, Integer> requests();

        static NamespaceAndRequests of(Namespace namespace, Map<GenericNamedMinTimestamp, Integer> requests) {
            return ImmutableNamespaceAndRequests.of(namespace, requests);
        }
    }
}
