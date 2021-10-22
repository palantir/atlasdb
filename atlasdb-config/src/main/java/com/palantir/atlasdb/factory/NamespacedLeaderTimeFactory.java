/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import com.palantir.atlasdb.config.TimeLockRequestBatcherProviders;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.lock.client.LeaderTimeCoalescingBatcher;
import com.palantir.lock.client.LeaderTimeGetter;
import com.palantir.lock.client.NamespacedCoalescingLeaderTimeGetter;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.client.ReferenceTrackingWrapper;
import java.util.function.Supplier;

public final class NamespacedLeaderTimeFactory implements LeaderTimeFactory {
    private final String timelockNamespace;
    private final TimeLockRequestBatcherProviders timeLockRequestBatcherProviders;
    private final Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier;

    public NamespacedLeaderTimeFactory(
            String timelockNamespace,
            TimeLockRequestBatcherProviders timeLockRequestBatcherProviders,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier) {
        this.timelockNamespace = timelockNamespace;
        this.timeLockRequestBatcherProviders = timeLockRequestBatcherProviders;
        this.multiClientTimelockServiceSupplier = multiClientTimelockServiceSupplier;
    }

    @Override
    public LeaderTimeGetter leaderTimeGetter(NamespacedConjureTimelockService namespacedConjureTimelockService) {
        ReferenceTrackingWrapper<LeaderTimeCoalescingBatcher> referenceTrackingBatcher =
                getReferenceTrackingWrapper(timeLockRequestBatcherProviders, multiClientTimelockServiceSupplier);
        referenceTrackingBatcher.recordReference();

        return new NamespacedCoalescingLeaderTimeGetter(timelockNamespace, referenceTrackingBatcher);
    }

    private static ReferenceTrackingWrapper<LeaderTimeCoalescingBatcher> getReferenceTrackingWrapper(
            TimeLockRequestBatcherProviders timelockRequestBatcherProviders,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier) {
        return timelockRequestBatcherProviders.leaderTime().getBatcher(multiClientTimelockServiceSupplier);
    }
}
