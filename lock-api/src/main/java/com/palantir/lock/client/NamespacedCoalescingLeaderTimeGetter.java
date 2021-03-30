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

import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.v2.LeaderTime;

/**
 * This class maintains the context of namespace for a client and directs leaderTime requests to
 * {@link LeaderTimeCoalescingBatcher} that coalesces and batches requests across clients.
 *
 * This is different from {@link LegacyLeaderTimeGetter} in that LegacyLeaderTimeGetter coalesces requests only for a
 * single namespace.
 */
public class NamespacedCoalescingLeaderTimeGetter implements LeaderTimeGetter {
    private final ReferenceTrackingWrapper<LeaderTimeCoalescingBatcher> referenceTrackingBatcher;
    private final Namespace namespace;

    public NamespacedCoalescingLeaderTimeGetter(
            String namespace, ReferenceTrackingWrapper<LeaderTimeCoalescingBatcher> referenceTrackingBatcher) {
        this.namespace = Namespace.of(namespace);
        this.referenceTrackingBatcher = referenceTrackingBatcher;
    }

    @Override
    public LeaderTime leaderTime() {
        return referenceTrackingBatcher.getDelegate().apply(namespace);
    }

    @Override
    public void close() {
        referenceTrackingBatcher.close();
    }
}
