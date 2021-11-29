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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.ManagedTimestampService;

public final class TransactionSchemaVersionEnforcement {
    private static final SafeLogger log = SafeLoggerFactory.get(TransactionSchemaVersionEnforcement.class);

    private static final int MAX_ATTEMPTS = 50;
    private static final long ONE_BILLION = 1_000_000_000L;

    private TransactionSchemaVersionEnforcement() {
        // utility
    }

    public static void ensureTransactionsGoingForwardHaveSchemaVersion(
            TransactionSchemaManager transactionSchemaManager,
            ManagedTimestampService timestampManagementService,
            int targetSchemaVersion) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            transactionSchemaManager.tryInstallNewTransactionsSchemaVersion(targetSchemaVersion);
            advanceTimestamp(timestampManagementService);

            int currentSchemaVersion = transactionSchemaManager.getTransactionsSchemaVersion(
                    timestampManagementService.getFreshTimestamp());
            if (currentSchemaVersion == targetSchemaVersion) {
                log.info(
                        "Enforced schema version to the target.",
                        SafeArg.of("targetSchemaVersion", targetSchemaVersion));
                return;
            }
        }
        log.error(
                "Could not enforce the desired schema version in spite of repeated retries.",
                SafeArg.of("numAttempts", MAX_ATTEMPTS));
    }

    private static void advanceTimestamp(ManagedTimestampService timestampManagementService) {
        timestampManagementService.fastForwardTimestamp(timestampManagementService.getFreshTimestamp() + ONE_BILLION);
    }
}
