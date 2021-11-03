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

import com.palantir.sls.versions.OrderableSlsVersion;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GreenNodeLeadershipStateTest {
    private static final OrderableSlsVersion VERSION = OrderableSlsVersion.valueOf("3.14.15");
    private static final OrderableSlsVersion NEW_VERSION = OrderableSlsVersion.valueOf("3.141.59");
    private static final long INITIAL_TIME = 100L;
    private static final long SECOND_TIME = 200L;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private GreenNodeLeadershipState state;

    @Before
    public void setup() {
        DataSource dataSource = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.getRoot().toPath());
        state = GreenNodeLeadershipState.create(dataSource);
    }

    @Test
    public void initialLatestAttemptTimeIsAbsent() {
        assertThat(state.getLatestAttemptTime(VERSION)).isEmpty();
    }

    @Test
    public void canUpdateLatestAttemptTimeMultipleTimes() {
        state.setLatestAttemptTime(VERSION, INITIAL_TIME);
        assertThat(state.getLatestAttemptTime(VERSION)).contains(INITIAL_TIME);

        state.setLatestAttemptTime(VERSION, SECOND_TIME);
        assertThat(state.getLatestAttemptTime(VERSION)).contains(SECOND_TIME);
    }

    @Test
    public void latestAttemptTimeIsAbsentForDifferentVersion() {
        state.setLatestAttemptTime(VERSION, INITIAL_TIME);

        assertThat(state.getLatestAttemptTime(NEW_VERSION)).isEmpty();
    }

    @Test
    public void updatingToNewVersionClearsPreviousVersion() {
        state.setLatestAttemptTime(VERSION, INITIAL_TIME);
        state.setLatestAttemptTime(NEW_VERSION, SECOND_TIME);

        assertThat(state.getLatestAttemptTime(NEW_VERSION)).contains(SECOND_TIME);
        assertThat(state.getLatestAttemptTime(VERSION)).isEmpty();
    }
}
