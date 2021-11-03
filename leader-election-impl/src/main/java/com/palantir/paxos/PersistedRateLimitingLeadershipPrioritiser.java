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

package com.palantir.paxos;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.time.Clock;
import com.palantir.common.time.SystemClock;
import com.palantir.sls.versions.OrderableSlsVersion;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import javax.sql.DataSource;

public final class PersistedRateLimitingLeadershipPrioritiser implements GreenNodeLeadershipPrioritiser {
    private final Optional<OrderableSlsVersion> timeLockVersion;
    private final Supplier<Duration> leadershipAttemptBackoff;
    private final GreenNodeLeadershipAttemptHistory greenNodeLeadershipAttemptHistory;
    private final Clock clock;

    @VisibleForTesting
    PersistedRateLimitingLeadershipPrioritiser(
            Optional<OrderableSlsVersion> timeLockVersion,
            Supplier<Duration> leadershipAttemptBackoff,
            GreenNodeLeadershipAttemptHistory greenNodeLeadershipAttemptHistory,
            Clock clock) {
        this.timeLockVersion = timeLockVersion;
        this.leadershipAttemptBackoff = leadershipAttemptBackoff;
        this.greenNodeLeadershipAttemptHistory = greenNodeLeadershipAttemptHistory;
        this.clock = clock;
    }

    public static PersistedRateLimitingLeadershipPrioritiser create(
            Optional<OrderableSlsVersion> timeLockVersion,
            Supplier<Duration> leadershipAttemptBackoff,
            DataSource sqliteDataSource) {
        GreenNodeLeadershipAttemptHistory greenNodeLeadershipAttemptHistory =
                GreenNodeLeadershipAttemptHistory.create(sqliteDataSource);
        return new PersistedRateLimitingLeadershipPrioritiser(
                timeLockVersion, leadershipAttemptBackoff, greenNodeLeadershipAttemptHistory, new SystemClock());
    }

    @Override
    public boolean shouldGreeningNodeBecomeLeader() {
        OrderableSlsVersion currentVersion = timeLockVersion.orElse(null);
        Optional<Long> latestAttemptTime = greenNodeLeadershipAttemptHistory.getLatestAttemptTime(currentVersion);
        if (latestAttemptTime.isEmpty()) {
            greenNodeLeadershipAttemptHistory.setLatestAttemptTime(currentVersion, clock.getTimeMillis());
            return true;
        }

        Instant latestAttempt = Instant.ofEpochMilli(latestAttemptTime.get());
        Instant currentTime = clock.instant();
        Duration durationSinceLatestAttempt = Duration.between(latestAttempt, currentTime);
        if (durationSinceLatestAttempt.compareTo(leadershipAttemptBackoff.get()) > 0) {
            greenNodeLeadershipAttemptHistory.setLatestAttemptTime(currentVersion, currentTime.toEpochMilli());
            return true;
        }

        return false;
    }
}
