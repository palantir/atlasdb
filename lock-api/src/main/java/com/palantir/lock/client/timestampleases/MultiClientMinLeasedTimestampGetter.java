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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

public final class MultiClientMinLeasedTimestampGetter implements AutoCloseable {
    private final DisruptorAutobatcher<MinLeasedTimestampGetterBatchParams, Long> disruptor;

    private MultiClientMinLeasedTimestampGetter(
            DisruptorAutobatcher<MinLeasedTimestampGetterBatchParams, Long> disruptor) {
        this.disruptor = disruptor;
    }

    static MultiClientMinLeasedTimestampGetter create(InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<MinLeasedTimestampGetterBatchParams, Long> disruptor = Autobatchers.coalescing(
                        new MinLeasedTimestampGetterBatchHandler(delegate))
                .safeLoggablePurpose("min-leased-timestamp-getter")
                .batchFunctionTimeout(Duration.ofSeconds(30))
                .build();

        return new MultiClientMinLeasedTimestampGetter(disruptor);
    }

    public Map<TimestampLeaseName, Long> getMinLeasedTimestamps(
            Namespace namespace, Set<TimestampLeaseName> timestampNames) {
        Map<TimestampLeaseName, ListenableFuture<Long>> futures = KeyedStream.of(timestampNames)
                .map(timestampName -> MinLeasedTimestampGetterBatchParams.of(namespace, timestampName))
                .map(this::getMinLeasedTimestamp)
                .collectToMap();

        return AtlasFutures.getUnchecked(AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()));
    }

    @Override
    public void close() {
        disruptor.close();
    }

    private ListenableFuture<Long> getMinLeasedTimestamp(MinLeasedTimestampGetterBatchParams params) {
        return disruptor.apply(params);
    }
}
