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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.keyvalue.api.ImmutableSweepResults;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper;
import com.palantir.atlasdb.persistentlock.KvsBackedPersistentLockServiceClientTest;
import com.palantir.atlasdb.sweep.priority.ImmutableUpdateSweepPriority;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.sweeperservice.SweeperService;
import com.palantir.remoting2.clients.UserAgents;
import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.jaxrs.JaxRsClient;
import com.palantir.remoting2.servers.jersey.HttpRemotingJerseyFeature;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class SweeperServiceImplTest extends SweeperTestSetup {
    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName(
            "sweeperservice.fasttest");
    private static KeyValueService kvs = Mockito.mock(KeyValueService.class);
    private static SweepProgressStore progressStore = Mockito.mock(SweepProgressStore.class);
    private static SweepPriorityStore priorityStore = Mockito.mock(SweepPriorityStore.class);
    private static SweepTaskRunner sweepTaskRunner = Mockito.mock(SweepTaskRunner.class);
    private static SweepMetrics sweepMetrics = Mockito.mock(SweepMetrics.class);
    private static long currentTimeMillis = 1000200300L;
    private static ImmutableSweepBatchConfig sweepBatchConfig = ImmutableSweepBatchConfig.builder()
            .deleteBatchSize(100)
            .candidateBatchSize(200)
            .maxCellTsPairsToExamine(1000)
            .build();

    private static SpecificTableSweeper specificTableSweeper = new SpecificTableSweeper(
            SweeperTestSetup.mockTxManager(),
            kvs,
            sweepTaskRunner,
            () -> sweepBatchConfig,
            priorityStore,
            progressStore,
            Mockito.mock(BackgroundSweeperPerformanceLogger.class),
            sweepMetrics,
            () -> currentTimeMillis);

    @ClassRule
    public static DropwizardClientRule dropwizardClientRule = new DropwizardClientRule(
            new SweeperServiceImpl(specificTableSweeper),
            new CheckAndSetExceptionMapper(),
            HttpRemotingJerseyFeature.DEFAULT);

    SweeperService sweeperService = JaxRsClient.builder().build(
            SweeperService.class,
            UserAgents.fromClass(KvsBackedPersistentLockServiceClientTest.class, "test", "unknown"),
            dropwizardClientRule.baseUri().toString());

    @Test(expected = RemoteException.class)
    public void sweepingNonFullyTableShouldNotBeSuccessful() {
        sweeperService.sweepTable("not_fqtn");
    }


    @Test(expected = RemoteException.class)
    public void sweepingNonExistingTableShouldNotBeSuccessful() {
        sweeperService.sweepTable("ns.non_existing_table");
    }

    @Test
    public void testWritePriorityNotUpdatedAfterCompleteFreshRun() {
        setNoProgress(progressStore);
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .build(), sweepTaskRunner, TABLE_REF);
        when(kvs.getAllTableNames()).thenReturn(ImmutableSet.of(TABLE_REF));
        sweeperService.sweepTable(TABLE_REF.getQualifiedName());
        Mockito.verify(priorityStore, never()).update(
                Mockito.any(),
                Mockito.eq(TABLE_REF),
                Mockito.eq(ImmutableUpdateSweepPriority.builder()
                        .newStaleValuesDeleted(2)
                        .newCellTsPairsExamined(10)
                        .newMinimumSweptTimestamp(12345L)
                        .newLastSweepTimeMillis(currentTimeMillis)
                        .newWriteCount(0L)
                        .build()));
    }

    @Test
    public void testWritePriorityNotUpdatedAfterSecondRunCompletesSweep() {
        byte[] startRow = {1, 2, 3};
        setProgress(progressStore, ImmutableSweepProgress.builder()
                .tableRef(TABLE_REF)
                .staleValuesDeleted(3)
                .cellTsPairsExamined(11)
                .minimumSweptTimestamp(4567L)
                .startRow(startRow)
                .build());
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(9999L)
                .previousStartRow(startRow)
                .build(), sweepTaskRunner, TABLE_REF);
        when(kvs.getAllTableNames()).thenReturn(ImmutableSet.of(TABLE_REF));
        sweeperService.sweepTableFromStartRow(TABLE_REF.getQualifiedName(), encodeStartRow(startRow));
        Mockito.verify(priorityStore, never()).update(
                Mockito.any(),
                Mockito.eq(TABLE_REF),
                Mockito.eq(ImmutableUpdateSweepPriority.builder()
                        .newStaleValuesDeleted(5)
                        .newCellTsPairsExamined(21)
                        .newMinimumSweptTimestamp(4567L)
                        .newLastSweepTimeMillis(currentTimeMillis)
                        .build()));
    }

    @Test
    public void testNotPutZeroWriteCountAfterFreshIncompleteRun() {
        setNoProgress(progressStore);
        byte[] nextStartRow = {1, 2, 3};
        setupTaskRunner(ImmutableSweepResults.builder()
                .staleValuesDeleted(2)
                .cellTsPairsExamined(10)
                .sweptTimestamp(12345L)
                .nextStartRow(nextStartRow)
                .build(), sweepTaskRunner, TABLE_REF);
        sweeperService.sweepTableFromStartRowWithBatchConfig(TABLE_REF.getQualifiedName(), encodeStartRow(nextStartRow), 10, 10, 1);
        when(kvs.getAllTableNames()).thenReturn(ImmutableSet.of(TABLE_REF));
        sweeperService.sweepTableFromStartRow(TABLE_REF.getQualifiedName(), encodeStartRow(nextStartRow));
        Mockito.verify(priorityStore, never()).update(
                Mockito.any(),
                Mockito.eq(TABLE_REF),
                Mockito.eq(ImmutableUpdateSweepPriority.builder()
                        .newWriteCount(0L)
                        .build()));
    }

    private String encodeStartRow(byte[] rowBytes) {
        return BaseEncoding.base16().encode(rowBytes);
    }

}
