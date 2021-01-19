/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import com.lmax.disruptor.WaitStrategy;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.v2.LeaderTime;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

public class LeaderTimeCoalescingBatcher implements AutoCloseable {
    private final DisruptorAutobatcher<Namespace, LeaderTime> batcher;

    public LeaderTimeCoalescingBatcher(
            InternalMultiClientConjureTimelockService delegate, OptionalInt bufferSize, WaitStrategy strategy) {
        this.batcher = Autobatchers.coalescing(new LeaderTimeCoalescingConsumer(delegate))
                .bufferSize(bufferSize)
                .waitStrategy(strategy)
                .safeLoggablePurpose("get-leader-times")
                .build();
    }

    public LeaderTime apply(Namespace namespace) {
        return AtlasFutures.getUnchecked(batcher.apply(namespace));
    }

    @Override
    public void close() {
        batcher.close();
    }

    static class LeaderTimeCoalescingConsumer implements CoalescingRequestFunction<Namespace, LeaderTime> {
        private final InternalMultiClientConjureTimelockService delegate;

        public LeaderTimeCoalescingConsumer(InternalMultiClientConjureTimelockService delegate) {
            this.delegate = delegate;
        }

        @Override
        public Map<Namespace, LeaderTime> apply(Set<Namespace> namespaces) {
            return delegate.leaderTimes(namespaces).getLeaderTimes();
        }
    }
}
