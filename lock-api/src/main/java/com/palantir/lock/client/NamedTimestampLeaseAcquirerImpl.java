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

import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.atlasdb.timelock.api.GenericNamedMinTimestamp;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.v2.NamedTimestampLeaseResult;
import com.palantir.lock.v2.PartialNamedTimestampLeaseResult;
import com.palantir.timestamp.TimestampRange;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;

public final class NamedTimestampLeaseAcquirerImpl implements NamedTimestampLeaseAcquirer {
    private final Namespace namespace;
    private final MultiClientNamedTimestampLeaseAcquirerProvider delegate;
    private final Supplier<TimestampRange> freshTimestampRangeSupplier;

    private NamedTimestampLeaseAcquirerImpl(Namespace namespace, MultiClientNamedTimestampLeaseAcquirerProvider delegate, Supplier<TimestampRange> freshTimestampRangeSupplier) {
        this.namespace = namespace;
        this.delegate = delegate;
        this.freshTimestampRangeSupplier = freshTimestampRangeSupplier;
    }

    public static NamedTimestampLeaseAcquirer create(String timelockNamespace, ReferenceTrackingWrapper<MultiClientNamedTimestampLeaseAcquirer> batcher, NamespacedConjureTimelockService namespacedConjureTimelockService) {
        MultiClientNamedTimestampLeaseAcquirerProvider delegate = new MultiClientNamedTimestampLeaseAcquirerProvider() {
            @Override
            public MultiClientNamedTimestampLeaseAcquirer get() {
                return batcher.getDelegate();
            }

            @Override
            public void close() {
                batcher.close();
            }
        };
        return new NamedTimestampLeaseAcquirerImpl(
                Namespace.of(timelockNamespace),
                delegate,
                () -> getFreshTimestamps(namespacedConjureTimelockService)
        );
    }

    public static NamedTimestampLeaseAcquirer create(String timelockNamespace, Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier, NamespacedConjureTimelockService namespacedConjureTimelockService) {
        MultiClientNamedTimestampLeaseAcquirer delegate = MultiClientNamedTimestampLeaseAcquirer.create(multiClientTimelockServiceSupplier.get());
        MultiClientNamedTimestampLeaseAcquirerProvider delegateProvider = new MultiClientNamedTimestampLeaseAcquirerProvider() {
            @Override
            public MultiClientNamedTimestampLeaseAcquirer get() {
                return delegate;
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
        return new NamedTimestampLeaseAcquirerImpl(
                Namespace.of(timelockNamespace),
                delegateProvider,
                () -> getFreshTimestamps(namespacedConjureTimelockService)
        );
    }

    @Override
    public Map<GenericNamedMinTimestamp, NamedTimestampLeaseResult> acquireNamedTimestampLease(
            Map<GenericNamedMinTimestamp, Integer> requests) {
        Map<GenericNamedMinTimestamp, PartialNamedTimestampLeaseResult> results = delegate.get().acquireNamedTimestampLeased(namespace, requests);
        return KeyedStream.stream(results)
                .map(this::createFromPartialResult)
                .collectToMap();
    }

    private NamedTimestampLeaseResult createFromPartialResult(PartialNamedTimestampLeaseResult partialResult) {
        return NamedTimestampLeaseResult.of(
                partialResult.minLeasedTimestamp(),
                partialResult.lock(),
                createFreshTimestampSupplier(partialResult.freshTimestampRange(), freshTimestampRangeSupplier)
        );
    }

    @Override
    public void close() {
        delegate.close();
    }

    // needs to be tested heavily!
    private static Supplier<Long> createFreshTimestampSupplier(TimestampRange starterRange, Supplier<TimestampRange> rangeSupplier) {
        // still figuring out what how to deal with the possible concurrency here
        // either weaker consistency, or assume no one will call this method concurrently
        return new Supplier<>() {
            @GuardedBy("this")
            private TimestampRange currentRange = starterRange;

            @GuardedBy("this")
            private long givenFromRange = 0L;

            @Override
            public synchronized Long get() {
                if (givenFromRange == currentRange.size()) {
                    currentRange = rangeSupplier.get();
                    givenFromRange = 0L;
                }

                long nextTimestamp = currentRange.getLowerBound() + givenFromRange;
                givenFromRange += 1;
                return nextTimestamp;
            }
        };
    }

    private static TimestampRange getFreshTimestamps(NamespacedConjureTimelockService service) {
        ConjureTimestampRange range = service.getFreshTimestampsV2(ConjureGetFreshTimestampsRequestV2.of(100)).get();
        return TimestampRange.createRangeFromDeltaEncoding(range.getStart(), range.getCount());
    }

}
