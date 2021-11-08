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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.palantir.common.time.Clock;
import com.palantir.sls.versions.OrderableSlsVersion;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PersistedRateLimitingLeadershipPrioritiserTest {
    private static final OrderableSlsVersion VERSION = OrderableSlsVersion.valueOf("3.14.15");
    private static final long INITIAL_TIME_MILLIS = 100L;
    private static final long BACKOFF_MILLIS = 1000L;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Mock
    private Clock clock;

    private PersistedRateLimitingLeadershipPrioritiser persistedRateLimitingLeadershipPrioritiser;

    @Before
    public void setup() {
        DataSource dataSource = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.getRoot().toPath());
        GreenNodeLeadershipAttemptHistory greenNodeLeadershipAttemptHistory =
                GreenNodeLeadershipAttemptHistory.create(dataSource);
        persistedRateLimitingLeadershipPrioritiser = new PersistedRateLimitingLeadershipPrioritiser(
                Optional.of(VERSION),
                () -> Duration.ofMillis(BACKOFF_MILLIS),
                greenNodeLeadershipAttemptHistory,
                clock);

        when(clock.instant()).thenReturn(Instant.ofEpochMilli(INITIAL_TIME_MILLIS));
    }

    @Test
    public void shouldBecomeLeaderWhenNoAttemptTimeStored() {
        assertThat(persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isTrue();
    }

    @Test
    public void shouldNotBecomeLeaderTwice() {
        assertThat(persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isTrue();
        assertThat(persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isFalse();
    }

    @Test
    public void shouldNotBecomeLeaderWithinBackoffPeriod() {
        // store initial state
        persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader();

        Instant withinBackoffTime = Instant.ofEpochMilli(INITIAL_TIME_MILLIS + (BACKOFF_MILLIS / 2));
        when(clock.instant()).thenReturn(withinBackoffTime);

        assertThat(persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isFalse();
    }

    @Test
    public void shouldBecomeLeaderAgainOutsideBackoffPeriod() {
        // store initial state
        persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader();

        Instant afterBackoffTime = Instant.ofEpochMilli(INITIAL_TIME_MILLIS + BACKOFF_MILLIS + 1);
        when(clock.instant()).thenReturn(afterBackoffTime);

        assertThat(persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isTrue();
    }

    @Test
    public void shouldNotBecomeLeaderAgainWithinSecondBackoffPeriod() {
        persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader();

        Instant afterBackoffTime = Instant.ofEpochMilli(INITIAL_TIME_MILLIS + BACKOFF_MILLIS + 1);
        when(clock.instant()).thenReturn(afterBackoffTime);

        assertThat(persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isTrue();
        assertThat(persistedRateLimitingLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isFalse();
    }
}
