/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.store;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatus.Aborted;
import com.palantir.atlasdb.transaction.service.TransactionStatus.Committed;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public final class AtlasDbTransactionConcluder {
    private static final int MAX_RETRIES = 5;

    private final TransactionService transactionService;

    public AtlasDbTransactionConcluder(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    public TransactionStatus forceTransactionConclusion(long startTimestamp) {
        TransactionStatus transactionStatus = transactionService.getV2(startTimestamp);
        if (isConclusiveTransactionStatus(transactionStatus)) {
            return transactionStatus;
        }

        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            if (attemptToForceTransactionConclusion(startTimestamp)) {
                return TransactionStatus.aborted();
            } else {
                transactionStatus = transactionService.getV2(startTimestamp);
                if (isConclusiveTransactionStatus(transactionStatus)) {
                    return transactionStatus;
                }
                // Otherwise try again: this can happen if we race with e.g. a Transactions4 start
            }
        }

        throw new SafeIllegalStateException(
                "Failed to force transaction conclusion", SafeArg.of("retries", MAX_RETRIES));
    }

    // Returns true iff we successfully aborted the transaction at this start timestamp
    private boolean attemptToForceTransactionConclusion(long startTimestamp) {
        try {
            transactionService.putUnlessExists(startTimestamp, TransactionConstants.FAILED_COMMIT_TS);
            return true;
        } catch (KeyAlreadyExistsException keyAlreadyExistsException) {
            // Transaction was already concluded, but we should perform another get
            return false;
        }
    }

    private static boolean isConclusiveTransactionStatus(TransactionStatus status) {
        return status instanceof Committed || status instanceof Aborted;
    }
}
