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
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.ReadOnlyTransactionSchemaManager;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timestamp.TimestampService;

public final class TransactionServices {
    private TransactionServices() {
        // Utility class
    }

    public static TransactionService createTransactionService(
            KeyValueService keyValueService, CoordinationService<InternalSchemaMetadata> coordinationService) {
        if (keyValueService.supportsCheckAndSet()) {
            return createSplitKeyTransactionService(keyValueService, coordinationService);
        }
        return createV1TransactionService(keyValueService);
    }

    private static TransactionService createSplitKeyTransactionService(
            KeyValueService keyValueService,
            CoordinationService<InternalSchemaMetadata> coordinationService) {
        TransactionSchemaManager transactionSchemaManager = new TransactionSchemaManager(coordinationService);
        return new SplitKeyDelegatingTransactionService<>(
                transactionSchemaManager::getTransactionsSchemaVersion,
                ImmutableMap.of(1, createV1TransactionService(keyValueService))
        );
    }

    public static TransactionService createV1TransactionService(KeyValueService keyValueService) {
        return new SimpleTransactionService(keyValueService);
    }

    /**
     * This method should only be used to create {@link TransactionService}s for testing, because in production
     * there are intermediate services like the {@link CoordinationService} this creates where metrics or other
     * forms of lifecycle management may be useful.
     */
    public static TransactionService createForTesting(
            KeyValueService keyValueService, TimestampService timestampService, boolean initializeAsync) {
        CoordinationService<InternalSchemaMetadata> coordinationService
                = CoordinationServices.createDefault(keyValueService, timestampService, initializeAsync);
        return createTransactionService(keyValueService, coordinationService);
    }

    public static TransactionService createReadOnlyTransactionServiceIgnoresUncommittedTransactionsDoesNotRollBack(
            KeyValueService keyValueService) {
        if (keyValueService.supportsCheckAndSet()) {
            CoordinationService<InternalSchemaMetadata> coordinationService = CoordinationServices.createDefault(
                    keyValueService,
                    () -> {
                        throw new SafeIllegalStateException("Attempted to get a timestamp from a read-only"
                                + " transaction service! This is probably a product bug. Please contact"
                                + " support.");
                        },
                    false);
            ReadOnlyTransactionSchemaManager readOnlyTransactionSchemaManager
                    = new ReadOnlyTransactionSchemaManager(coordinationService);
            return new SplitKeyDelegatingTransactionService<>(
                    readOnlyTransactionSchemaManager::getTransactionsSchemaVersion,
                    ImmutableMap.of(1, createV1TransactionService(keyValueService))
            );
        }
        return createV1TransactionService(keyValueService);
    }
}
