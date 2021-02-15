/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import java.util.function.Supplier;

public class LeaderClock {
    private final LeadershipId leadershipId;
    private final Supplier<NanoTime> clock;

    @VisibleForTesting
    LeaderClock(LeadershipId leadershipId, Supplier<NanoTime> clock) {
        this.leadershipId = leadershipId;
        this.clock = clock;
    }

    public static LeaderClock create() {
        return new LeaderClock(LeadershipId.random(), NanoTime::now);
    }

    public LeaderTime time() {
        return LeaderTime.of(leadershipId, clock.get());
    }

    public LeadershipId id() {
        return leadershipId;
    }
}
