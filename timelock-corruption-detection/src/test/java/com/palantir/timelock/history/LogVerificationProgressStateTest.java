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
import com.palantir.timelock.history.models.ProgressState;
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

    private LogVerificationProgressState log;

    @Before
    public void setup() {
        DataSource dataSource =
                SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());
        log = LogVerificationProgressState.create(dataSource);
    }

    @Test
    public void initialStateIsAbsent() {
        assertThat(log.getProgressComponents(CLIENT, USE_CASE)).isEmpty();
    }

    @Test
    public void canResetState() {
        long greatestLogSeq = 55L;
        ProgressState progress = log.resetProgressState(CLIENT, USE_CASE, greatestLogSeq);

        assertThat(progress.lastVerifiedSeq()).isEqualTo(-1L);
        assertThat(progress.progressLimit()).isEqualTo(greatestLogSeq);
    }

    @Test
    public void canUpdateState() {
        long greatestLogSeq = 7L;
        long progressState = 5L;

        log.resetProgressState(CLIENT, USE_CASE, greatestLogSeq);

        assertThat(log.getProgressComponents(CLIENT, USE_CASE))
                .hasValue(ProgressState.builder()
                        .progressLimit(greatestLogSeq)
                        .lastVerifiedSeq(-1L)
                        .build());

        log.updateProgress(CLIENT, USE_CASE, progressState);
        assertThat(log.getProgressComponents(CLIENT, USE_CASE))
                .hasValue(ProgressState.builder()
                        .progressLimit(greatestLogSeq)
                        .lastVerifiedSeq(progressState)
                        .build());
    }
}
