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

package com.palantir.lock.client;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BroadsideLeaderPoller {
    private static final Logger log = LoggerFactory.getLogger(BroadsideLeaderPoller.class);
    private static final int MAXIMUM_ATTEMPTS = 5;

    private final Supplier<LeaderTimes> leaderTimesSupplier;
    private final Set<Namespace> namespaces;

    @VisibleForTesting
    BroadsideLeaderPoller(Supplier<LeaderTimes> leaderTimesSupplier, Set<Namespace> namespaces) {
        this.leaderTimesSupplier = leaderTimesSupplier;
        this.namespaces = namespaces;
    }

    public static BroadsideLeaderPoller create(InternalMultiClientConjureTimelockService timelockService) {
        Set<Namespace> namespaces = ConcurrentHashMap.newKeySet();
        return new BroadsideLeaderPoller(
                new CoalescingSupplier<>(() -> timelockService.leaderTimes(namespaces)), namespaces);
    }

    public LeaderTime get(Namespace namespace) {
        namespaces.add(namespace);
        for (int attempt = 1; attempt <= MAXIMUM_ATTEMPTS; attempt++) {
            Map<Namespace, LeaderTime> leaderTimes = leaderTimesSupplier.get().getLeaderTimes();
            LeaderTime leaderTime = leaderTimes.get(namespace);
            if (leaderTime != null) {
                return leaderTime;
            }
            log.info("Failed to get leader time for a namespace. This is unexpected and probably indicates an"
                    + " AtlasDB bug, but we will try again as this is likely to be transient.",
                    SafeArg.of("namespace", namespace));
        }
        log.warn(
                "Failed to get leader time for a namespace, despite multiple attempts!",
                SafeArg.of("namespace", namespace));
        throw new SafeIllegalStateException(
                "Failed to get leader time for a namespace", SafeArg.of("namespace", namespace));
    }
}
