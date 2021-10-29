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
package com.palantir.atlasdb.transaction.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.ReadOnlyTransactionSchemaManager;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timestamp.TimestampService;
import java.util.Map;

public final class TransactionServices {
    private TransactionServices() {
        // Utility class
    }

    public static TransactionService createTransactionService(
            KeyValueService keyValueService, TransactionSchemaManager transactionSchemaManager) {
        if (keyValueService.getCheckAndSetCompatibility() == CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE) {
            return createSplitKeyTransactionService(keyValueService, transactionSchemaManager);
        }
        return createV1TransactionService(keyValueService);
    }

    private static TransactionService createSplitKeyTransactionService(
            KeyValueService keyValueService, TransactionSchemaManager transactionSchemaManager) {
        // TODO (jkong): Is there a way to disallow DIRECT -> V2 transaction service in the map?
        return new PreStartHandlingTransactionService(new SplitKeyDelegatingTransactionService<>(
                transactionSchemaManager::getTransactionsSchemaVersion,
                ImmutableMap.of(
                        TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION,
                        createV1TransactionService(keyValueService),
                        TransactionConstants.TICKETS_ENCODING_TRANSACTIONS_SCHEMA_VERSION,
                        createV2TransactionService(keyValueService))));
    }

    public static TransactionService createV1TransactionService(KeyValueService keyValueService) {
        return new PreStartHandlingTransactionService(SimpleTransactionService.createV1(keyValueService));
    }

    private static TransactionService createV2TransactionService(KeyValueService keyValueService) {
        return new PreStartHandlingTransactionService(
                WriteBatchingTransactionService.create(SimpleTransactionService.createV2(keyValueService)));
    }

    private static TransactionService createV3TransactionService(KeyValueService keyValueService) {

        return new PreStartHandlingTransactionService(
                WriteBatchingTransactionService.create(PueTablingTransactionService.createV3(keyValueService)));
    }

    /**
     * This method should only be used to create {@link TransactionService}s for testing, because in production there
     * are intermediate services like the {@link CoordinationService} this creates where metrics or other forms of
     * lifecycle management may be useful.
     */
    public static TransactionService createRaw(
            KeyValueService keyValueService, TimestampService timestampService, boolean initializeAsync) {
        CoordinationService<InternalSchemaMetadata> coordinationService = CoordinationServices.createDefault(
                keyValueService, timestampService, MetricsManagers.createForTests(), initializeAsync);
        return createTransactionService(keyValueService, new TransactionSchemaManager(coordinationService));
    }

    public static TransactionService createReadOnlyTransactionServiceIgnoresUncommittedTransactionsDoesNotRollBack(
            KeyValueService keyValueService, MetricsManager metricsManager) {
        if (keyValueService.supportsCheckAndSet()) {
            CoordinationService<InternalSchemaMetadata> coordinationService = CoordinationServices.createDefault(
                    keyValueService,
                    () -> {
                        throw new SafeIllegalStateException("Attempted to get a timestamp from a read-only"
                                + " transaction service! This is probably a product bug. Please contact"
                                + " support.");
                    },
                    metricsManager,
                    false);
            ReadOnlyTransactionSchemaManager readOnlyTransactionSchemaManager =
                    new ReadOnlyTransactionSchemaManager(coordinationService);
            return new PreStartHandlingTransactionService(new SplitKeyDelegatingTransactionService<>(
                    readOnlyTransactionSchemaManager::getTransactionsSchemaVersion,
                    ImmutableMap.of(1, createV1TransactionService(keyValueService))));
        }
        return createV1TransactionService(keyValueService);
    }

    /**
     * Constructs an {@link AsyncTransactionService} such that methods are blocking and return immediate futures.
     *
     * @param transactionService on which to call synchronous requests
     * @return {@link AsyncTransactionService} which delegates to synchronous methods
     */
    public static AsyncTransactionService synchronousAsAsyncTransactionService(TransactionService transactionService) {
        return new AsyncTransactionService() {
            @Override
            public ListenableFuture<Long> getAsync(long startTimestamp) {
                return Futures.immediateFuture(transactionService.get(startTimestamp));
            }

            @Override
            public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
                return Futures.immediateFuture(transactionService.get(startTimestamps));
            }
        };
    }
}
