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

package com.palantir.atlasdb.transaction.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.palantir.atlasdb.atomic.AtomicValue;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.encoding.BaseProgressEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TransactionStatusEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V4ProgressEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionStatusUtils;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.timelock.paxos.InMemoryTimeLockRule;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TransactionServicesTest {
    private final KeyValueService keyValueService = spy(new InMemoryKeyValueService(false));

    private TimestampService timestampService;
    private CoordinationService<InternalSchemaMetadata> coordinationService;
    private TransactionService transactionService;

    private TransactionKnowledgeComponents knowledge;

    private long startTs;
    private long commitTs;

    @ClassRule
    public static InMemoryTimeLockRule services = new InMemoryTimeLockRule();

    @Before
    public void setUp() {
        TransactionTables.createTables(keyValueService);
        MetricsManager metricsManager = MetricsManagers.createForTests();

        timestampService = services.getTimestampService();
        coordinationService =
                CoordinationServices.createDefault(keyValueService, timestampService, metricsManager, false);
        knowledge = TransactionKnowledgeComponents.createForTests(keyValueService, metricsManager.getTaggedRegistry());
        transactionService = TransactionServices.createTransactionService(
                keyValueService, new TransactionSchemaManager(coordinationService), knowledge);
    }

    @Test
    public void valuesPutMayBeSubsequentlyRetrievedV1() {
        initializeTimestamps();
        transactionService.putUnlessExists(startTs, commitTs);
        assertThat(transactionService.get(startTs)).isEqualTo(commitTs);
    }

    @Test
    public void valuesPutMayBeSubsequentlyRetrievedV2() {
        forceInstallV2();
        initializeTimestamps();
        transactionService.putUnlessExists(startTs, commitTs);
        assertThat(transactionService.get(startTs)).isEqualTo(commitTs);
    }

    @Test
    public void cannotPutValuesTwiceV1() {
        initializeTimestamps();
        assertCannotPutValuesTwice();
    }

    @Test
    public void cannotPutValuesTwiceV2() {
        forceInstallV2();
        initializeTimestamps();
        assertCannotPutValuesTwice();
    }

    private void assertCannotPutValuesTwice() {
        transactionService.putUnlessExists(startTs, commitTs);
        assertThatThrownBy(() -> transactionService.putUnlessExists(startTs, commitTs))
                .isInstanceOf(KeyAlreadyExistsException.class)
                .hasMessageContaining("already have a value for this timestamp");
        assertThat(transactionService.get(startTs)).isEqualTo(commitTs);
    }

    @Test
    public void commitsV1TransactionByDefault() {
        initializeTimestamps();
        transactionService.putUnlessExists(startTs, commitTs);

        Map<Cell, byte[]> actualArgument = verifyPueInTableAndReturnArgument(TransactionConstants.TRANSACTION_TABLE);
        assertExpectedArgument(actualArgument, V1EncodingStrategy.INSTANCE);

        verify(keyValueService, never()).putUnlessExists(eq(TransactionConstants.TRANSACTIONS2_TABLE), anyMap());
    }

    @Test
    public void canCommitV2Transaction() {
        forceInstallV2();
        initializeTimestamps();
        transactionService.putUnlessExists(startTs, commitTs);

        Map<Cell, byte[]> actualArgument = verifyPueInTableAndReturnArgument(TransactionConstants.TRANSACTIONS2_TABLE);
        assertExpectedArgument(actualArgument, TicketsEncodingStrategy.INSTANCE);

        verify(keyValueService, never()).putUnlessExists(eq(TransactionConstants.TRANSACTION_TABLE), anyMap());
    }

    @Test
    public void markV3TransactionIsNoop() {
        forceInstallV3();
        initializeTimestamps();
        transactionService.markInProgress(startTs);

        verify(keyValueService, never()).put(eq(TransactionConstants.TRANSACTIONS2_TABLE), any(), anyLong());
    }

    @Test
    public void canCommitV3TransactionWithoutMarking() {
        forceInstallV3();
        initializeTimestamps();
        transactionService.putUnlessExists(startTs, commitTs);

        Map<Cell, byte[]> actualArgument = verifyPueInTableAndReturnArgument(TransactionConstants.TRANSACTIONS2_TABLE);
        assertExpectedArgumentTwoPhase(
                actualArgument, new TwoPhaseEncodingStrategy(BaseProgressEncodingStrategy.INSTANCE));

        verify(keyValueService, never()).putUnlessExists(eq(TransactionConstants.TRANSACTION_TABLE), anyMap());
    }

    @Test
    public void canMarkV4TransactionInProgress() {
        forceInstallV4();
        initializeTimestamps();
        transactionService.markInProgress(startTs);

        Map<Cell, byte[]> actualArgument =
                verifyPutInTableAndReturnArgument(TransactionConstants.TRANSACTIONS2_TABLE, 0);
        Cell cell =
                new TwoPhaseEncodingStrategy(V4ProgressEncodingStrategy.INSTANCE).encodeStartTimestampAsCell(startTs);

        assertThat(actualArgument.keySet()).containsExactly(cell);
        assertThat(actualArgument.get(cell)).containsExactly(TransactionConstants.TTS_IN_PROGRESS_MARKER);
    }

    @Test
    public void canReadInProgressStatusV4Transaction() {
        forceInstallV4();
        initializeTimestamps();
        transactionService.markInProgress(startTs);
        assertThat(transactionService.get(startTs)).isNull();
        assertThat(transactionService.getV2(startTs)).isEqualTo(TransactionStatuses.inProgress());
    }

    @Test
    public void cannotCommitV4TransactionWithoutMarking() {
        forceInstallV4();
        initializeTimestamps();
        transactionService.putUnlessExists(startTs, commitTs);

        assertThatThrownBy(() -> transactionService.getV2(startTs)).isInstanceOf(KeyAlreadyExistsException.class);
    }

    @Test
    public void canCommitV4Transaction() {
        forceInstallV4();
        initializeTimestamps();
        transactionService.markInProgress(startTs);
        transactionService.putUnlessExists(startTs, commitTs);

        TwoPhaseEncodingStrategy strategy = new TwoPhaseEncodingStrategy(V4ProgressEncodingStrategy.INSTANCE);
        Cell cell = strategy.encodeStartTimestampAsCell(startTs);
        byte[] update = strategy.encodeCommitStatusAsValue(
                startTs, AtomicValue.staging(TransactionStatuses.committed(commitTs)));

        MultiCheckAndSetRequest actualMcasRequest = verifyMcasInTableAndReturnArgument();
        assertThat(actualMcasRequest.tableRef()).isEqualTo(TransactionConstants.TRANSACTIONS2_TABLE);
        assertThat(ByteBuffer.wrap(actualMcasRequest.rowName())).isEqualTo(ByteBuffer.wrap(cell.getRowName()));

        assertThat(actualMcasRequest.expected().keySet()).containsExactly(cell);
        assertThat(actualMcasRequest.expected().get(cell)).containsExactly(TransactionConstants.TTS_IN_PROGRESS_MARKER);

        assertThat(actualMcasRequest.updates().keySet()).containsExactly(cell);
        assertThat(actualMcasRequest.updates().get(cell)).containsExactly(update);
    }

    private void initializeTimestamps() {
        startTs = timestampService.getFreshTimestamp();
        commitTs = timestampService.getFreshTimestamp();
    }

    private void forceInstallV2() {
        forceInstallVersion(2);
    }

    private void forceInstallV3() {
        forceInstallVersion(3);
    }

    private void forceInstallV4() {
        forceInstallVersion(4);
    }

    private void forceInstallVersion(int newVersion) {
        TransactionSchemaManager transactionSchemaManager = new TransactionSchemaManager(coordinationService);
        Awaitility.await().atMost(Duration.ofSeconds(200)).until(() -> {
            transactionSchemaManager.tryInstallNewTransactionsSchemaVersion(newVersion);
            ((TimestampManagementService) timestampService)
                    .fastForwardTimestamp(timestampService.getFreshTimestamp() + 1_000_000);
            return transactionSchemaManager.getTransactionsSchemaVersion(timestampService.getFreshTimestamp())
                    == newVersion;
        });
    }

    private Map<Cell, byte[]> verifyPutInTableAndReturnArgument(TableReference tableReference, long timestamp) {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<Cell, byte[]>> argument = ArgumentCaptor.forClass(Map.class);
        verify(keyValueService).put(eq(tableReference), argument.capture(), eq(timestamp));
        return argument.getValue();
    }

    private Map<Cell, byte[]> verifyPueInTableAndReturnArgument(TableReference tableReference) {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<Cell, byte[]>> argument = ArgumentCaptor.forClass(Map.class);
        verify(keyValueService).putUnlessExists(eq(tableReference), argument.capture());
        return argument.getValue();
    }

    private MultiCheckAndSetRequest verifyMcasInTableAndReturnArgument() {
        ArgumentCaptor<MultiCheckAndSetRequest> mcasRequestCaptor =
                ArgumentCaptor.forClass(MultiCheckAndSetRequest.class);
        verify(keyValueService).multiCheckAndSet(mcasRequestCaptor.capture());
        return mcasRequestCaptor.getValue();
    }

    private void assertExpectedArgument(
            Map<Cell, byte[]> actualArgument, TransactionStatusEncodingStrategy<TransactionStatus> strategy) {
        Cell cell = strategy.encodeStartTimestampAsCell(startTs);
        byte[] value = strategy.encodeCommitStatusAsValue(startTs, TransactionStatusUtils.fromTimestamp(commitTs));

        assertThat(actualArgument.keySet()).containsExactly(cell);
        assertThat(actualArgument.get(cell)).containsExactly(value);
    }

    private void assertExpectedArgumentTwoPhase(Map<Cell, byte[]> actualArgument, TwoPhaseEncodingStrategy strategy) {
        Cell cell = strategy.encodeStartTimestampAsCell(startTs);
        byte[] value = strategy.encodeCommitStatusAsValue(
                startTs, AtomicValue.staging(TransactionStatusUtils.fromTimestamp(commitTs)));

        assertThat(actualArgument.keySet()).containsExactly(cell);
        assertThat(actualArgument.get(cell)).containsExactly(value);
    }
}
