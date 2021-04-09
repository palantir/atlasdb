/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TimestampEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TransactionServicesTest {
    private final KeyValueService keyValueService = spy(new InMemoryKeyValueService(false));
    private final TimestampService timestampService = new InMemoryTimestampService();
    private final CoordinationService<InternalSchemaMetadata> coordinationService = CoordinationServices.createDefault(
            keyValueService, timestampService, MetricsManagers.createForTests(), false);
    private final TransactionService transactionService = TransactionServices.createTransactionService(
            keyValueService, new TransactionSchemaManager(coordinationService));

    private long startTs;
    private long commitTs;

    @Before
    public void setup() {
        TransactionTables.createTables(keyValueService);
    }

    @Test
    public void valuesPutMayBeSubsequentlyRetrievedV1() {
        initializeTimestamps();
        transactionService.putUnlessExists(startTs, commitTs);
        assertThat(transactionService.get(startTs)).isEqualTo(commitTs);
    }

    @Test
    public void transactionTableLoggingArgsAreSafe() {
        assertThat(LoggingArgs.row(TransactionConstants.TRANSACTIONS2_TABLE, new byte[] {1}, x -> x))
                .isInstanceOf(SafeArg.class);
        assertThat(LoggingArgs.column(TransactionConstants.TRANSACTIONS2_TABLE, new byte[] {2}, x -> x))
                .isInstanceOf(SafeArg.class);
        assertThat(LoggingArgs.value(TransactionConstants.TRANSACTIONS2_TABLE,
                Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("c")), new byte[] {3}, x -> x))
                .isInstanceOf(SafeArg.class);

        assertThat(LoggingArgs.row(TransactionConstants.TRANSACTION_TABLE, new byte[] {1}, x -> x))
                .isInstanceOf(SafeArg.class);
        assertThat(LoggingArgs.column(TransactionConstants.TRANSACTION_TABLE, new byte[] {2}, x -> x))
                .isInstanceOf(SafeArg.class);
        assertThat(LoggingArgs.value(TransactionConstants.TRANSACTION_TABLE,
                Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("c")), new byte[] {3}, x -> x))
                .isInstanceOf(SafeArg.class);

        TableReference randomTable =
                TableReference.createWithEmptyNamespace("t" + ThreadLocalRandom.current().nextLong());
        assertThat(LoggingArgs.row(randomTable, new byte[] {1}, x -> x))
                .isInstanceOf(UnsafeArg.class);
        assertThat(LoggingArgs.column(randomTable, new byte[] {2}, x -> x))
                .isInstanceOf(UnsafeArg.class);
        assertThat(LoggingArgs.value(randomTable,
                Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("c")), new byte[] {3}, x -> x))
                .isInstanceOf(UnsafeArg.class);
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

    private void initializeTimestamps() {
        startTs = timestampService.getFreshTimestamp();
        commitTs = timestampService.getFreshTimestamp();
    }

    private void forceInstallV2() {
        TransactionSchemaManager transactionSchemaManager = new TransactionSchemaManager(coordinationService);
        Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> {
            transactionSchemaManager.tryInstallNewTransactionsSchemaVersion(2);
            ((TimestampManagementService) timestampService)
                    .fastForwardTimestamp(timestampService.getFreshTimestamp() + 1_000_000);
            return transactionSchemaManager.getTransactionsSchemaVersion(timestampService.getFreshTimestamp()) == 2;
        });
    }

    private Map<Cell, byte[]> verifyPueInTableAndReturnArgument(TableReference tableReference) {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<Cell, byte[]>> argument = ArgumentCaptor.forClass(Map.class);
        verify(keyValueService).putUnlessExists(eq(tableReference), argument.capture());
        return argument.getValue();
    }

    private void assertExpectedArgument(Map<Cell, byte[]> actualArgument, TimestampEncodingStrategy strategy) {
        Cell cell = strategy.encodeStartTimestampAsCell(startTs);
        byte[] value = strategy.encodeCommitTimestampAsValue(startTs, commitTs);

        assertThat(actualArgument.keySet()).containsExactly(cell);
        assertThat(actualArgument.get(cell)).containsExactly(value);
    }
}
