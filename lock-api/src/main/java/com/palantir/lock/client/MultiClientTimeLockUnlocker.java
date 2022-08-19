/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import org.immutables.value.Value;

public class MultiClientTimeLockUnlocker implements AutoCloseable {
    private final DisruptorAutobatcher<UnlockRequest, Set<ConjureLockTokenV2>> batcher;

    public MultiClientTimeLockUnlocker(InternalMultiClientConjureTimelockService delegate, OptionalInt bufferSize) {
        this.batcher = Autobatchers.independent(new UnlockConsumer(delegate))
                .bufferSize(bufferSize)
                .batchFunctionTimeout(Duration.ofSeconds(30))
                .safeLoggablePurpose("multi-client-timelock-unlocker")
                .build();
    }

    public Set<ConjureLockTokenV2> unlock(Namespace namespace, Set<ConjureLockTokenV2> tokens) {
        return AtlasFutures.getUnchecked(batcher.apply(ImmutableUnlockRequest.of(namespace, tokens)));
    }

    @Override
    public void close() {
        batcher.close();
    }

    private static final class SingleClientBatchManager {
        private final List<BatchElement<UnlockRequest, Set<ConjureLockTokenV2>>> requests;

        private SingleClientBatchManager() {
            this.requests = new ArrayList<>();
        }

        private void addBatchElement(BatchElement<UnlockRequest, Set<ConjureLockTokenV2>> batchElement) {
            requests.add(batchElement);
        }

        private ConjureUnlockRequestV2 getCombinedRequest() {
            Set<ConjureLockTokenV2> lockTokens = new HashSet<>();
            for (BatchElement<UnlockRequest, Set<ConjureLockTokenV2>> batchElement : requests) {
                UnlockRequest lockSet = batchElement.argument();
                lockTokens.addAll(lockSet.lockSet());
            }
            return ConjureUnlockRequestV2.of(lockTokens);
        }

        public void applyResponse(ConjureUnlockResponseV2 relevantResponse) {
            Set<ConjureLockTokenV2> unlockedTokens = new HashSet<>(relevantResponse.get());
            for (BatchElement<UnlockRequest, Set<ConjureLockTokenV2>> batchElement : requests) {
                Set<ConjureLockTokenV2> plausibleUnlocks = ImmutableSet.copyOf(
                        Sets.intersection(batchElement.argument().lockSet(), unlockedTokens));
                batchElement.result().set(plausibleUnlocks);
                unlockedTokens.removeAll(plausibleUnlocks);
            }
        }
    }

    @VisibleForTesting
    static class UnlockConsumer implements Consumer<List<BatchElement<UnlockRequest, Set<ConjureLockTokenV2>>>> {
        private final InternalMultiClientConjureTimelockService timelockService;

        public UnlockConsumer(InternalMultiClientConjureTimelockService timelockService) {
            this.timelockService = timelockService;
        }

        @Override
        public void accept(List<BatchElement<UnlockRequest, Set<ConjureLockTokenV2>>> batchElements) {
            Map<Namespace, SingleClientBatchManager> batchManagers = new HashMap<>();
            for (BatchElement<UnlockRequest, Set<ConjureLockTokenV2>> batchElement : batchElements) {
                UnlockRequest unlockRequest = batchElement.argument();
                batchManagers
                        .computeIfAbsent(unlockRequest.namespace(), unused -> new SingleClientBatchManager())
                        .addBatchElement(batchElement);
            }

            Map<Namespace, ConjureUnlockResponseV2> responses = timelockService.unlock(KeyedStream.stream(batchManagers)
                    .map(SingleClientBatchManager::getCombinedRequest)
                    .collectToMap());

            for (Map.Entry<Namespace, SingleClientBatchManager> batchManagerEntry : batchManagers.entrySet()) {
                ConjureUnlockResponseV2 relevantResponse = responses.get(batchManagerEntry.getKey());
                batchManagerEntry.getValue().applyResponse(relevantResponse);
            }
        }
    }

    @Value.Immutable
    interface UnlockRequest {
        @Value.Parameter
        Namespace namespace();

        @Value.Parameter
        Set<ConjureLockTokenV2> lockSet();
    }
}
