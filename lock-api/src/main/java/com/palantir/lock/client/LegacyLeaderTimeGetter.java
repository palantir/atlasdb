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

import com.palantir.common.concurrent.ParameterizedCoalescingSupplier;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.logsafe.SafeArg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LegacyLeaderTimeGetter implements LeaderTimeGetter {
    private static final Logger log = LoggerFactory.getLogger(LegacyLeaderTimeGetter.class);

    private final ParameterizedCoalescingSupplier<LeaderTime> time;
    private final String namespace;

    public LegacyLeaderTimeGetter(NamespacedConjureTimelockService delegate, String namespace) {
        this.time = new ParameterizedCoalescingSupplier<>(namespace, delegate::leaderTime);
        this.namespace = namespace;
    }

    @Override
    public LeaderTime leaderTime() {
        long startTime = System.nanoTime();
        LeaderTime leaderTime = time.get();
        log.info(
                "The start - {} and end times - {} to fetch leaderTime using "
                        + "respective coalescingSupplier call for namespace - {}",
                SafeArg.of("startTime", startTime),
                SafeArg.of("endTime", System.nanoTime()),
                SafeArg.of("leaderTimeGetterReqNamespace", namespace));
        return leaderTime;
    }

    @Override
    public void close() {}
}
