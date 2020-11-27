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

import static com.palantir.timelock.history.PaxosLogHistoryProgressTracker.MAX_ROWS_ALLOWED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.SqliteConnections;
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
    private static final SqlitePaxosStateLogHistory SQLITE_PAXOS_STATE_LOG_HISTORY =
            mock(SqlitePaxosStateLogHistory.class);

    private PaxosLogHistoryProgressTracker progressTracker;
    private LogVerificationProgressState log;

    @Before
    public void setup() {
        DataSource dataSource =
                SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());
        log = LogVerificationProgressState.create(dataSource);
        progressTracker = new PaxosLogHistoryProgressTracker(dataSource, SQLITE_PAXOS_STATE_LOG_HISTORY);
    }

    @Test
    public void getsInitialStateBoundsWhenNoDataInDB() {
        assertSanityOfFetchedHistoryQuerySeqBounds(0, MAX_ROWS_ALLOWED - 1);
    }

    @Test
    public void getsCorrectStateBoundsWhenDataInDBButNotInCache() {
        long greatestLogSeq = 7L;
        long lastVerified = 3L;

        when(SQLITE_PAXOS_STATE_LOG_HISTORY.getGreatestLogEntry(any(), any())).thenReturn(greatestLogSeq);
        log.updateProgress(CLIENT, USE_CASE, lastVerified);

        assertSanityOfFetchedHistoryQuerySeqBounds(lastVerified + 1, lastVerified + MAX_ROWS_ALLOWED);
    }

    @Test
    public void canUpdateProgressState() {
        long greatestLogSeq = 100L;
        long upper = 50L;
        long lastVerified = 50L;

        HistoryQuerySequenceBounds bounds = HistoryQuerySequenceBounds.builder()
                .lowerBoundInclusive(5L)
                .upperBoundInclusive(upper)
                .build();

        when(SQLITE_PAXOS_STATE_LOG_HISTORY.getGreatestLogEntry(any(), any())).thenReturn(greatestLogSeq);
        progressTracker.updateProgressStateForNamespaceAndUseCase(NAMESPACE_AND_USE_CASE, bounds);

        assertSanityOfFetchedHistoryQuerySeqBounds(lastVerified + 1, lastVerified + MAX_ROWS_ALLOWED);
    }

    @Test
    public void canResetProgressStateIfRequired() {
        long greatestLogSeq = 100L;
        when(SQLITE_PAXOS_STATE_LOG_HISTORY.getGreatestLogEntry(any(), any())).thenReturn(greatestLogSeq);

        HistoryQuerySequenceBounds bounds = HistoryQuerySequenceBounds.builder()
                .lowerBoundInclusive(1L)
                .upperBoundInclusive(greatestLogSeq + 1)
                .build();

        progressTracker.updateProgressStateForNamespaceAndUseCase(NAMESPACE_AND_USE_CASE, bounds);
        assertSanityOfFetchedHistoryQuerySeqBounds(0, MAX_ROWS_ALLOWED - 1);
    }

    public void assertSanityOfFetchedHistoryQuerySeqBounds(long lowerBoundInclusive, long upperBoundInclusive) {
        HistoryQuerySequenceBounds paxosLogSequenceBounds =
                progressTracker.getNextPaxosLogSequenceRangeToBeVerified(NAMESPACE_AND_USE_CASE);
        assertThat(paxosLogSequenceBounds.getLowerBoundInclusive()).isEqualTo(lowerBoundInclusive);
        assertThat(paxosLogSequenceBounds.getUpperBoundInclusive()).isEqualTo(upperBoundInclusive);
    }
}
