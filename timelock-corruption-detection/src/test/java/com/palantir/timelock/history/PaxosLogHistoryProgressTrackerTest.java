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

import static com.palantir.timelock.history.models.SequenceBounds.MAX_ROWS_ALLOWED;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.SqliteConnections;
import com.palantir.timelock.history.models.SequenceBounds;
import com.palantir.timelock.history.sqlite.LogVerificationProgressState;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosLogHistoryProgressTrackerTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("tom");
    private static final String USE_CASE = "useCase1";
    private static final NamespaceAndUseCase NAMESPACE_AND_USE_CASE = ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE);

    private PaxosLogHistoryProgressTracker progressTracker;
    private LogVerificationProgressState log;

    @Before
    public void setup() {
        DataSource dataSource =
                SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());
        log = LogVerificationProgressState.create(dataSource);
        progressTracker = new PaxosLogHistoryProgressTracker(dataSource, SqlitePaxosStateLogHistory.create(dataSource));
    }

    @Test
    public void getsInitialStateBoundsWhenNoDataInDB() {
        SequenceBounds paxosLogSequenceBounds = progressTracker.getPaxosLogSequenceBounds(NAMESPACE_AND_USE_CASE);
        assertThat(paxosLogSequenceBounds.lowerInclusive()).isEqualTo(-1L);
        assertThat(paxosLogSequenceBounds.upperInclusive()).isEqualTo(-1L + MAX_ROWS_ALLOWED);
    }

    @Test
    public void getsCorrectStateBoundsWhenDataInDBButNotInCache() {
        long greatestLogSeq = 7L;
        long progressState = 3L;

        log.resetProgressState(CLIENT, USE_CASE, greatestLogSeq);
        log.updateProgress(CLIENT, USE_CASE, progressState);

        SequenceBounds paxosLogSequenceBounds = progressTracker.getPaxosLogSequenceBounds(NAMESPACE_AND_USE_CASE);
        assertThat(paxosLogSequenceBounds.lowerInclusive()).isEqualTo(progressState);
        assertThat(paxosLogSequenceBounds.upperInclusive()).isEqualTo(progressState + MAX_ROWS_ALLOWED);
    }

    @Test
    public void canUpdateProgressState() {
        long greatestLogSeq = 100L;
        long upper = 50L;
        SequenceBounds bounds = SequenceBounds.builder()
                .lowerInclusive(5L)
                .upperInclusive(upper)
                .build();

        log.resetProgressState(CLIENT, USE_CASE, greatestLogSeq);
        progressTracker.updateProgressStateForNamespaceAndUseCase(NAMESPACE_AND_USE_CASE, bounds);

        SequenceBounds paxosLogSequenceBounds = progressTracker.getPaxosLogSequenceBounds(NAMESPACE_AND_USE_CASE);
        assertThat(paxosLogSequenceBounds.lowerInclusive()).isEqualTo(upper);
        assertThat(paxosLogSequenceBounds.upperInclusive()).isEqualTo(upper + MAX_ROWS_ALLOWED);
    }

    @Test
    public void canResetProgressStateIfRequired() {
        long greatestLogSeq = 100L;
        SequenceBounds bounds = SequenceBounds.builder()
                .lowerInclusive(1L)
                .upperInclusive(greatestLogSeq + 1)
                .build();

        log.resetProgressState(CLIENT, USE_CASE, greatestLogSeq);
        progressTracker.updateProgressStateForNamespaceAndUseCase(NAMESPACE_AND_USE_CASE, bounds);

        SequenceBounds paxosLogSequenceBounds = progressTracker.getPaxosLogSequenceBounds(NAMESPACE_AND_USE_CASE);
        assertThat(paxosLogSequenceBounds.lowerInclusive()).isEqualTo(-1L);
        assertThat(paxosLogSequenceBounds.upperInclusive()).isEqualTo(-1L + MAX_ROWS_ALLOWED);
    }
}
