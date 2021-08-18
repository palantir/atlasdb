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

package com.palantir.paxos;

import static com.palantir.paxos.PaxosStateLogTestUtils.NAMESPACE;
import static com.palantir.paxos.PaxosStateLogTestUtils.valueForRound;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SplittingPaxosStateLogTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private PaxosStateLog<PaxosValue> legacyLog;
    private PaxosStateLog<PaxosValue> currentLog;
    private AtomicInteger writeMetric = new AtomicInteger(0);
    private AtomicInteger readMetric = new AtomicInteger(0);

    @Before
    public void setup() throws IOException {
        DataSource legacy = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.newFolder("legacy").toPath());
        DataSource current = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.newFolder("current").toPath());
        legacyLog = spy(SqlitePaxosStateLog.create(NAMESPACE, legacy));
        currentLog = spy(SqlitePaxosStateLog.create(NAMESPACE, current));
    }

    @Test
    public void currentLogCanBeEmptyIffCutoffIsNoLogEntryValue() {
        assertThatCode(() -> SplittingPaxosStateLog.create(parametersWithCutoff(PaxosAcceptor.NO_LOG_ENTRY)))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> SplittingPaxosStateLog.create(parametersWithCutoff(7L)))
                .isInstanceOf(SafeIllegalStateException.class);
    }

    @Test
    public void getGreatestLogEntryReturnsCurrentLogValue() {
        legacyLog.writeRound(1000L, valueForRound(1000L));
        currentLog.writeRound(500L, valueForRound(500L));
        PaxosStateLog<PaxosValue> splittingLog = SplittingPaxosStateLog.create(parametersWithCutoff(100L));

        assertThat(splittingLog.getGreatestLogEntry()).isEqualTo(500L);
    }

    @Test
    public void getLeastLogEntryReturnsLegacyLogValueWhenLessThanCutoff() {
        legacyLog.writeRound(10L, valueForRound(10L));
        currentLog.writeRound(500L, valueForRound(500L));
        PaxosStateLog<PaxosValue> splittingLog = SplittingPaxosStateLog.create(parametersWithCutoff(100L));

        assertThat(splittingLog.getLeastLogEntry()).isEqualTo(10L);
    }

    @Test
    public void getLeastLogEntryReturnsCutoffWhenLegacyLeastValueIsGreaterThanCutoff() {
        legacyLog.writeRound(110L, valueForRound(10L));
        currentLog.writeRound(500L, valueForRound(500L));
        PaxosStateLog<PaxosValue> splittingLog = SplittingPaxosStateLog.create(parametersWithCutoff(100L));

        assertThat(splittingLog.getLeastLogEntry()).isEqualTo(100L);
    }

    @Test
    public void writingRoundsBeforeCutoffCanAffectGetLeastValueButDelegatesOnlyOnce() {
        legacyLog.writeRound(1000L, valueForRound(1000L));
        currentLog.writeRound(500L, valueForRound(500L));
        PaxosStateLog<PaxosValue> splittingLog = SplittingPaxosStateLog.create(parametersWithCutoff(100L));
        assertThat(splittingLog.getGreatestLogEntry()).isEqualTo(500L);

        splittingLog.writeRound(50L, valueForRound(50L));
        assertThat(splittingLog.getLeastLogEntry()).isEqualTo(50L);
        splittingLog.writeRound(70L, valueForRound(70L));
        assertThat(splittingLog.getLeastLogEntry()).isEqualTo(50L);
        splittingLog.writeRound(20L, valueForRound(20L));
        assertThat(splittingLog.getLeastLogEntry()).isEqualTo(20L);
        verify(legacyLog, times(1)).getLeastLogEntry();
    }

    @Test
    public void writesAreDelegatedCorrectly() {
        currentLog.writeRound(500L, valueForRound(500L));
        PaxosStateLog<PaxosValue> splittingLog = SplittingPaxosStateLog.create(parametersWithCutoff(100L));

        splittingLog.writeRound(300L, valueForRound(300L));
        verify(legacyLog, never()).writeRound(300L, valueForRound(300L));
        verify(currentLog).writeRound(300L, valueForRound(300L));
        assertThat(writeMetric).hasValue(0);

        splittingLog.writeRound(100L, valueForRound(100L));
        verify(legacyLog, never()).writeRound(100L, valueForRound(100L));
        verify(currentLog).writeRound(100L, valueForRound(100L));
        assertThat(writeMetric).hasValue(0);

        splittingLog.writeRound(20L, valueForRound(20L));
        verify(legacyLog).writeRound(20L, valueForRound(20L));
        verify(currentLog, never()).writeRound(20L, valueForRound(20L));
        assertThat(writeMetric).hasValue(1);
    }

    @Test
    public void readsAreDelegatedCorrectly() throws IOException {
        currentLog.writeRound(500L, valueForRound(500L));
        PaxosStateLog<PaxosValue> splittingLog = SplittingPaxosStateLog.create(parametersWithCutoff(100L));

        splittingLog.readRound(500L);
        verify(legacyLog, never()).readRound(500L);
        verify(currentLog).readRound(500L);
        assertThat(readMetric).hasValue(0);

        splittingLog.readRound(100L);
        verify(legacyLog, never()).readRound(100L);
        verify(currentLog).readRound(100L);
        assertThat(readMetric).hasValue(0);

        splittingLog.readRound(20L);
        verify(legacyLog).readRound(20L);
        verify(currentLog, never()).readRound(20L);
        assertThat(readMetric).hasValue(1);
    }

    private SplittingPaxosStateLog.SplittingParameters<PaxosValue> parametersWithCutoff(long cutoff) {

        return ImmutableSplittingParameters.<PaxosValue>builder()
                .legacyLog(legacyLog)
                .currentLog(currentLog)
                .legacyOperationMarkers(ImmutableLegacyOperationMarkers.builder()
                        .markLegacyRead(readMetric::incrementAndGet)
                        .markLegacyWrite(writeMetric::incrementAndGet)
                        .build())
                .cutoffInclusive(cutoff)
                .build();
    }
}
