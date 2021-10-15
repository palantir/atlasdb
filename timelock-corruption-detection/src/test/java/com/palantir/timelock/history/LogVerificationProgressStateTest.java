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

package com.palantir.timelock.history;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.paxos.Client;
import com.palantir.paxos.SqliteConnections;
import com.palantir.timelock.history.sqlite.LogVerificationProgressState;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LogVerificationProgressStateTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("tom");
    private static final String USE_CASE = "useCase1";

    private DataSource dataSource;
    private LogVerificationProgressState log;

    @Before
    public void setup() {
        dataSource = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.getRoot().toPath());
        log = LogVerificationProgressState.create(dataSource);
    }

    @Test
    public void initialStateIsAbsent() {
        assertThat(log.getLastVerifiedSeq(CLIENT, USE_CASE)).isEqualTo(-1L);
    }

    @Test
    public void canUpdateState() {
        log.updateProgress(CLIENT, USE_CASE, 5L);
        assertThat(log.getLastVerifiedSeq(CLIENT, USE_CASE)).isEqualTo(5L);
    }
}
