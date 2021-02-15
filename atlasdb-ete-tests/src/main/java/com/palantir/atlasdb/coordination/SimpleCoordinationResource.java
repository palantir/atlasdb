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

package com.palantir.atlasdb.coordination;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.timestamp.TimestampService;

public final class SimpleCoordinationResource implements CoordinationResource {
    private static final TableReference TEST_TABLE =
            TableReference.createFromFullyQualifiedName("test." + SimpleCoordinationResource.class.getSimpleName());
    private static final Cell TEST_CELL = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col"));

    private final TransactionManager transactionManager;
    private final TransactionSchemaManager transactionSchemaManager;
    private final TimestampService timestampService;

    private SimpleCoordinationResource(
            TransactionManager transactionManager, TransactionSchemaManager transactionSchemaManager) {
        this.transactionManager = transactionManager;
        this.transactionSchemaManager = transactionSchemaManager;
        this.timestampService = transactionManager.getTimestampService();
    }

    public static CoordinationResource create(TransactionManager transactionManager) {
        return new SimpleCoordinationResource(
                transactionManager,
                new TransactionSchemaManager(CoordinationServices.createDefault(
                        transactionManager.getKeyValueService(),
                        transactionManager.getTimestampService(),
                        MetricsManagers.createForTests(),
                        false)));
    }

    @Override
    public int getTransactionsSchemaVersion(long timestamp) {
        return transactionSchemaManager.getTransactionsSchemaVersion(timestamp);
    }

    @Override
    public boolean tryInstallNewTransactionsSchemaVersion(int newVersion) {
        return transactionSchemaManager.tryInstallNewTransactionsSchemaVersion(newVersion);
    }

    @Override
    public void forceInstallNewTransactionsSchemaVersion(int newVersion) {
        boolean successful = false;
        while (!successful) {
            successful = transactionSchemaManager.tryInstallNewTransactionsSchemaVersion(newVersion);
            advanceOneHundredMillionTimestamps();
        }
    }

    @Override
    public boolean doTransactionAndReportOutcome() {
        try {
            return transactionManager.runTaskThrowOnConflict(tx -> {
                KeyValueService kvs = transactionManager.getKeyValueService();
                kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

                tx.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, new byte[1]));
                return true;
            });
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public long resetStateAndGetFreshTimestamp() {
        forceInstallNewTransactionsSchemaVersion(1);
        KeyValueService kvs = transactionManager.getKeyValueService();
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.truncateTable(TEST_TABLE);
        return timestampService.getFreshTimestamp();
    }

    @Override
    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    private void advanceOneHundredMillionTimestamps() {
        transactionManager
                .getTimestampManagementService()
                .fastForwardTimestamp(timestampService.getFreshTimestamp() + 100_000_000);
    }
}
