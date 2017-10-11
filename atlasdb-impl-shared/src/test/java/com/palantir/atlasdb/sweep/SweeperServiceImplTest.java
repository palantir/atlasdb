/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper;
import com.palantir.atlasdb.util.DropwizardClientRule;
import com.palantir.atlasdb.util.TestJaxRsClientFactory;
import com.palantir.remoting.api.errors.RemoteException;
import com.palantir.remoting3.servers.jersey.HttpRemotingJerseyFeature;

public class SweeperServiceImplTest extends SweeperTestSetup {

    private static final String VALID_START_ROW = "010203";
    private static final String INVALID_START_ROW = "xyz";
    SweeperService sweeperService;

    private static final SweepResults RESULTS_WITH_NO_MORE_TO_SWEEP = SweepResults.createEmptySweepResult();

    @Rule
    public DropwizardClientRule dropwizardClientRule = new DropwizardClientRule(
            new SweeperServiceImpl(getSpecificTableSweeperService()),
            new CheckAndSetExceptionMapper(),
            HttpRemotingJerseyFeature.INSTANCE);

    // This method overrides the SweeperTestSetup method. Not sure if this is the intention, but leaving
    // as such for now.
    @Override
    @Before
    public void setup() {
        sweeperService = TestJaxRsClientFactory.createJaxRsClientForTest(
                SweeperService.class,
                SweeperServiceImplTest.class,
                dropwizardClientRule.baseUri().toString());

        setupTaskRunner(RESULTS_WITH_NO_MORE_TO_SWEEP);
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
                .matches(ex -> ex.getStatus() == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    @Test
    public void sweepTableFromStartRowWithValidStartRowShouldBeSuccessful() {
        sweeperService.sweepTableFrom(TABLE_REF.getQualifiedName(), VALID_START_ROW);
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
        sweeperService.sweepTableFully(TABLE_REF.getQualifiedName(), Optional.empty(), Optional.empty(), Optional.of(1000),
                Optional.of(1000), Optional.of(500));
    }

    @Test
    public void sweepTableFromStartRowWithBatchConfigWithExactlyOneNonNullBatchConfigShouldBeSuccessful() {
        sweeperService.sweepTableFully(TABLE_REF.getQualifiedName(),
                Optional.of(encodeStartRow(new byte[] {1, 2, 3})), Optional.empty(), Optional.of(10), Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testWriteProgressOrPriorityOrMetricsNotUpdatedAfterSweepRunsSuccessfully() {
        sweeperService.sweepTableFrom(TABLE_REF.getQualifiedName(), encodeStartRow(new byte[] {1, 2, 3}));
        verify(priorityStore, never()).update(any(), any(), any());
        verify(progressStore, never()).saveProgress(any(), any());
        Mockito.verifyZeroInteractions(sweepMetrics);
    }

    @Test
    public void sweepsEntireTableByDefault() {
        List<byte[]> startRows = ImmutableList.of(
                PtBytes.EMPTY_BYTE_ARRAY,
                new byte[] {0x10},
                new byte[] {0x50});

        for (int i = 0; i < startRows.size(); i++) {
            byte[] currentRow = startRows.get(i);
            Optional<byte[]> nextRow = (i+1) == startRows.size()
                    ? Optional.empty()
                    : Optional.of(startRows.get(i+1));

            SweepResults results = SweepResults.createEmptySweepResult(nextRow);
            when(sweepTaskRunner.run(any(), any(), eq(currentRow))).thenReturn(results);
        }

        sweeperService.sweepTableFully(TABLE_REF.getQualifiedName());

        startRows.forEach(row -> verify(sweepTaskRunner).run(any(), any(), eq(row)));
        verifyNoMoreInteractions(sweepTaskRunner);
    }

    @Test
    public void runsOneIterationIfRequested() {
        SweepResults resultsWithMoreToSweep = SweepResults.createEmptySweepResult(Optional.of(new byte[] {0x55}));
        setupTaskRunner(resultsWithMoreToSweep);

        sweeperService.sweepTableFully(TABLE_REF.getQualifiedName(), Optional.empty(), Optional.of(false),
                Optional.empty(), Optional.empty(), Optional.empty());

        verify(sweepTaskRunner, times(1)).run(any(), any(), any());
        verifyNoMoreInteractions(sweepTaskRunner);
    }

    private String encodeStartRow(byte[] rowBytes) {
        return BaseEncoding.base16().encode(rowBytes);
    }

    private void verifyNoSweepResultsSaved() {
        verify(progressStore, never()).saveProgress(any(), any());
        verify(priorityStore, never()).update(any(), any(), any());
    }

}
