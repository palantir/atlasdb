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
public class DbGreenNodeLeadershipPrioritiserTest {
    private static final OrderableSlsVersion VERSION = OrderableSlsVersion.valueOf("3.14.15");
    private static final long INITIAL_TIME_MILLIS = 100L;
    private static final long BACKOFF_MILLIS = 1000L;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Mock
    private Clock clock;

    private DbGreenNodeLeadershipPrioritiser dbGreenNodeLeadershipPrioritiser;

    @Before
    public void setup() {
        DataSource dataSource = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.getRoot().toPath());
        GreenNodeLeadershipState greenNodeLeadershipState = GreenNodeLeadershipState.create(dataSource);
        dbGreenNodeLeadershipPrioritiser = new DbGreenNodeLeadershipPrioritiser(
                Optional.of(VERSION), () -> Duration.ofMillis(BACKOFF_MILLIS), greenNodeLeadershipState, clock);

        when(clock.getTimeMillis()).thenReturn(INITIAL_TIME_MILLIS);
    }

    @Test
    public void shouldBecomeLeaderWhenNoAttemptTimeStored() {
        assertThat(dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isTrue();
    }

    @Test
    public void shouldNotBecomeLeaderTwice() {
        assertThat(dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isTrue();
        assertThat(dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isFalse();
    }

    @Test
    public void shouldNotBecomeLeaderWithinBackoffPeriod() {
        // store initial state
        dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader();

        long withinBackoffTime = INITIAL_TIME_MILLIS + (BACKOFF_MILLIS / 2);
        when(clock.getTimeMillis()).thenReturn(withinBackoffTime);

        assertThat(dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isFalse();
    }

    @Test
    public void shouldBecomeLeaderAgainOutsideBackoffPeriod() {
        // store initial state
        dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader();

        long withinBackoffTime = INITIAL_TIME_MILLIS + BACKOFF_MILLIS + 1;
        when(clock.getTimeMillis()).thenReturn(withinBackoffTime);

        assertThat(dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isTrue();
    }

    @Test
    public void shouldNotBecomeLeaderAgainWithinSecondBackoffPeriod() {
        dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader();

        long withinBackoffTime = INITIAL_TIME_MILLIS + BACKOFF_MILLIS + 1;
        when(clock.getTimeMillis()).thenReturn(withinBackoffTime);

        assertThat(dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isTrue();
        assertThat(dbGreenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader())
                .isFalse();
    }
}
