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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.util.RateLimitedLogger;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import javax.sql.DataSource;

public final class PersistedRateLimitingLeadershipPrioritiser implements GreenNodeLeadershipPrioritiser {
    private static final SafeLogger log = SafeLoggerFactory.get(PersistedRateLimitingLeadershipPrioritiser.class);
    private static final RateLimitedLogger logger = new RateLimitedLogger(log, 1.0 / 60);

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
        Instant currentTime = clock.instant();
        boolean shouldBecomeLeader = shouldBecomeLeader(currentVersion, currentTime);

        if (shouldBecomeLeader) {
            greenNodeLeadershipAttemptHistory.setLatestAttemptTime(currentVersion, currentTime.toEpochMilli());
        }

        return shouldBecomeLeader;
    }

    private boolean shouldBecomeLeader(OrderableSlsVersion currentVersion, Instant currentTime) {
        Optional<Long> latestAttemptTime = greenNodeLeadershipAttemptHistory.getLatestAttemptTime(currentVersion);
        if (latestAttemptTime.isEmpty()) {
            log.info(
                    "Attempting to become the leader for the first time on this version",
                    SafeArg.of("version", currentVersion));
            return true;
        }

        Instant latestAttempt = Instant.ofEpochMilli(latestAttemptTime.get());
        Duration durationSinceLatestAttempt = Duration.between(latestAttempt, currentTime);
        boolean shouldBecomeLeader = durationSinceLatestAttempt.compareTo(leadershipAttemptBackoff.get()) > 0;
        if (shouldBecomeLeader) {
            log.info(
                    "Attempting to become the leader on this version again,"
                            + " as enough time has passed since the last attempt",
                    SafeArg.of("version", currentVersion),
                    SafeArg.of("durationSinceLastAttempt", durationSinceLatestAttempt));
        } else {
            logger.log(lg -> lg.info(
                    "Not attempting to become the leader on this version again,"
                            + " as not enough time has passed since the last attempt",
                    SafeArg.of("version", currentVersion),
                    SafeArg.of("durationSinceLastAttempt", durationSinceLatestAttempt)));
        }

        return shouldBecomeLeader;
    }
}
