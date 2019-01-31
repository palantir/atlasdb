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

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public class TransactionServicesTest {
    private static final int START_TS = 123;
    private static final int COMMIT_TS = 555;
    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final TimestampService timestampService = new InMemoryTimestampService();
    private final CoordinationService<InternalSchemaMetadata> coordinationService
            = CoordinationServices.createDefault(keyValueService, timestampService, false);
    private final TransactionService transactionService = TransactionServices.createTransactionService(
            keyValueService, coordinationService);

    @Test
    public void valuesPutMayBeSubsequentlyRetrieved() {
        transactionService.putUnlessExists(START_TS, COMMIT_TS);
        assertThat(transactionService.get(START_TS)).isEqualTo(COMMIT_TS);
    }

    @Test
    public void cannotPutValuesTwice() {
        transactionService.putUnlessExists(START_TS, COMMIT_TS);
        assertThatThrownBy(() -> transactionService.putUnlessExists(START_TS, COMMIT_TS + 1))
                .isInstanceOf(KeyAlreadyExistsException.class)
                .hasMessageContaining("already have a value for this timestamp");
        assertThat(transactionService.get(START_TS)).isEqualTo(COMMIT_TS);
    }

    @Test
    public void commitsV1TransactionByDefault() {
        long startTs = COMMIT_TS + 1;
        transactionService.putUnlessExists(startTs, startTs + 1);
        assertThat(transactionService.get(startTs)).isEqualTo(startTs + 1);

        Cell v1Cell = V1EncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTs);
        assertThat(keyValueService.get(TransactionConstants.TRANSACTION_TABLE, ImmutableMap.of(v1Cell, 1L)).get(v1Cell)
                .getContents()).containsExactly(
                V1EncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(startTs, startTs + 1));

        assertThat(keyValueService.get(TransactionConstants.TRANSACTION_TABLE,
                ImmutableMap.of(TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTs), 1L))).isEmpty();
    }

    @Test
    public void canCommitV2Transaction() {
        TransactionSchemaManager transactionSchemaManager = new TransactionSchemaManager(coordinationService);
        forceInstallV2(transactionSchemaManager);
        long startTs = timestampService.getFreshTimestamp();
        transactionService.putUnlessExists(startTs, startTs + 1);
        assertThat(transactionService.get(startTs)).isEqualTo(startTs + 1);

        assertThat(keyValueService.get(TransactionConstants.TRANSACTION_TABLE,
                ImmutableMap.of(V1EncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTs), 1L))).isEmpty();
        Cell v2Cell = TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTs);

        assertThat(keyValueService.get(TransactionConstants.TRANSACTION_TABLE, ImmutableMap.of(v2Cell, 1L)).get(v2Cell)
                .getContents()).containsExactly(
                TicketsEncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(startTs, startTs + 1));
    }

    private void forceInstallV2(TransactionSchemaManager transactionSchemaManager) {
        Awaitility.await().atMost(1, TimeUnit.SECONDS)
                .until(() -> {
                    transactionSchemaManager.tryInstallNewTransactionsSchemaVersion(2);
                    ((TimestampManagementService) timestampService).fastForwardTimestamp(
                            timestampService.getFreshTimestamp() + 1_000_000);
                    return transactionSchemaManager
                            .getTransactionsSchemaVersion(timestampService.getFreshTimestamp()) == 2;
                });
    }
}
