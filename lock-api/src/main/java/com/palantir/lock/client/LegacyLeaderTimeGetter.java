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

import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.lock.v2.LeaderTime;

public class LegacyLeaderTimeGetter implements LeaderTimeGetter {
    private final CoalescingSupplier<LeaderTime> time;

    public LegacyLeaderTimeGetter(NamespacedConjureTimelockService delegate) {
        this.time = new CoalescingSupplier<>(delegate::leaderTime);
    }

    @Override
    public LeaderTime leaderTime() {
        return time.get();
    }

    @Override
    public void close() {}
}
