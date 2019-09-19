/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper;
import com.palantir.atlasdb.util.DropwizardClientRule;
import com.palantir.atlasdb.util.TestJaxRsClientFactory;
import com.palantir.conjure.java.api.errors.RemoteException;
import com.palantir.conjure.java.server.jersey.ConjureJerseyFeature;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class SweeperServiceImplTest extends SweeperTestSetup {

    private static final String VALID_START_ROW = "0102030A";
    private static final String LOWERCASE_BUT_VALID_START_ROW = "abadcafe";
    private static final String MIXED_CASE_START_ROW = "AaAaAaAaAa1111";
    private static final String INVALID_START_ROW = "xyz";
    SweeperService sweeperService;

    private static final SweepResults RESULTS_NO_MORE_TO_SWEEP = SweepResults.createEmptySweepResultWithNoMoreToSweep();
    private static final SweepResults RESULTS_MORE_TO_SWEEP = SweepResults.createEmptySweepResultWithMoreToSweep();

    @Rule
    public DropwizardClientRule dropwizardClientRule = new DropwizardClientRule(
            new SweeperServiceImpl(getSpecificTableSweeperService(), sweepBatchConfigSource),
            new CheckAndSetExceptionMapper(),
            ConjureJerseyFeature.INSTANCE);

    @Override
    @Before
    public void setup() {
        super.setup();

        sweeperService = TestJaxRsClientFactory.createJaxRsClientForTest(
                SweeperService.class,
                SweeperServiceImplTest.class,
                dropwizardClientRule.baseUri().toString());

        setupTaskRunner(RESULTS_NO_MORE_TO_SWEEP);
        when(kvs.getAllTableNames()).thenReturn(ImmutableSet.of(TABLE_REF));
    }

    @After
    public void after() {
        verifyNoSweepResultsSaved();
    }

    @Test
    public void sweepingNonFullyTableShouldNotBeSuccessful() {
        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() -> sweeperService.sweepTableFully("non_fully_qualified_name"))
                .matches(ex -> ex.getStatus() == Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void sweepingNonExistingTableShouldNotBeSuccessful() {
        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() -> sweeperService.sweepTableFully("ns.non_existing_table"))
                .matches(ex -> ex.getStatus() == Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void sweepTableFromStartRowWithValidStartRowShouldBeSuccessful() {
        sweeperService.sweepTableFrom(TABLE_REF.getQualifiedName(), VALID_START_ROW);
    }

    @Test
    public void sweepTableFromStartRowShouldAcceptLowercaseBase16Encodings() {
        when(kvs.getAllTableNames()).thenReturn(ImmutableSet.of(TABLE_REF));

        sweeperService.sweepTableFrom(TABLE_REF.getQualifiedName(), LOWERCASE_BUT_VALID_START_ROW);
    }

    @Test
    public void sweepTableFromStartRowShouldAcceptMixedCaseBase16Encodings() {
        when(kvs.getAllTableNames()).thenReturn(ImmutableSet.of(TABLE_REF));

        sweeperService.sweepTableFrom(TABLE_REF.getQualifiedName(), MIXED_CASE_START_ROW);
    }

    @Test
    public void sweepTableFromStartRowWithInValidStartRowShouldThrow() {
        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() ->
                        sweeperService.sweepTableFrom(TABLE_REF.getQualifiedName(), INVALID_START_ROW))
                .matches(ex -> ex.getStatus() == Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void sweepTableFromStartRowWithBatchConfigWithNullStartRowShouldBeSuccessful() {
        sweeperService.sweepTable(TABLE_REF.getQualifiedName(), Optional.empty(), Optional.empty(), Optional.of(1000),
                Optional.of(1000), Optional.of(500));
    }

    @Test
    public void sweepTableFromStartRowWithBatchConfigWithExactlyOneNonNullBatchConfigShouldBeSuccessful() {
        sweeperService.sweepTable(TABLE_REF.getQualifiedName(),
                Optional.of(encodeStartRow(new byte[] {1, 2, 3})), Optional.empty(), Optional.of(10), Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testWriteProgressAndPriorityNotUpdatedAfterSweepRunsSuccessfully_butMetricsAre() {
        sweeperService.sweepTableFrom(TABLE_REF.getQualifiedName(), encodeStartRow(new byte[] {1, 2, 3}));
        ArgumentCaptor<Long> sweepTime = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> totalTimeElapsed = ArgumentCaptor.forClass(Long.class);

        Mockito.verify(sweepMetrics, times(1)).updateSweepTime(sweepTime.capture(), totalTimeElapsed.capture());
        verifyExpectedArgument(sweepTime.getValue(), totalTimeElapsed.getValue());
    }

    @Test
    public void sweepsEntireTableByDefault() {
        List<byte[]> startRows = ImmutableList.of(
                PtBytes.EMPTY_BYTE_ARRAY,
                new byte[] {0x10},
                new byte[] {0x50});

        for (int i = 0; i < startRows.size(); i++) {
            byte[] currentRow = startRows.get(i);
            Optional<byte[]> nextRow = (i + 1) == startRows.size()
                    ? Optional.empty()
                    : Optional.of(startRows.get(i + 1));

            SweepResults results = SweepResults.createEmptySweepResult(nextRow);
            when(sweepTaskRunner.run(any(), any(), eq(currentRow))).thenReturn(results);
        }

        sweeperService.sweepTableFully(TABLE_REF.getQualifiedName());

        startRows.forEach(row -> verify(sweepTaskRunner).run(any(), any(), eq(row)));
        verifyNoMoreInteractions(sweepTaskRunner);
    }


    @Test
    public void runsOneIterationIfRequested() {
        setupTaskRunner(RESULTS_MORE_TO_SWEEP);

        sweeperService.sweepTable(TABLE_REF.getQualifiedName(), Optional.empty(), Optional.of(false),
                Optional.empty(), Optional.empty(), Optional.empty());

        verify(sweepTaskRunner, times(1)).run(any(), any(), any());
        verifyNoMoreInteractions(sweepTaskRunner);
    }

    private void verifyExpectedArgument(long sweepTime, long totalTimeElapsed) {
        assertThat(RESULTS_NO_MORE_TO_SWEEP.getTimeInMillis()).isEqualTo(sweepTime);
        assertThat(totalTimeElapsed).isBetween(
                RESULTS_NO_MORE_TO_SWEEP.getTimeElapsedSinceStartedSweeping() - 1000L,
                RESULTS_NO_MORE_TO_SWEEP.getTimeElapsedSinceStartedSweeping());
    }

    private String encodeStartRow(byte[] rowBytes) {
        return BaseEncoding.base16().encode(rowBytes);
    }

    private void verifyNoSweepResultsSaved() {
        verify(progressStore, never()).saveProgress(any());
        verify(priorityStore, never()).update(any(), any(), any());
    }

}
