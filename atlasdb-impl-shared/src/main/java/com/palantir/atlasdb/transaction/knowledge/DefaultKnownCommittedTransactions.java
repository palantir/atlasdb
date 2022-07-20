/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions.Consistency;
import com.palantir.atlasdb.transaction.service.TransactionService;
import java.util.Optional;

public class DefaultKnownCommittedTransactions implements KnownCommittedTransactions {
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final KnownAbortedTransactions knownAbortedTransactions;
    private final TransactionService transactionService;

    public DefaultKnownCommittedTransactions(
            KnownConcludedTransactions knownConcludedTransactions,
            KnownAbortedTransactions knownAbortedTransactions,
            TransactionService transactionService) {
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.knownAbortedTransactions = knownAbortedTransactions;
        this.transactionService = transactionService;
    }

    @Override
    public boolean isKnownCommitted(long startTimestamp) {
        if (!isConcluded(startTimestamp)) {
            return Optional.ofNullable(transactionService.get(startTimestamp)).isPresent();
        }

        return !knownAbortedTransactions.isKnownAborted(startTimestamp);
    }

    private boolean isConcluded(long startTimestamp) {
        return knownConcludedTransactions.isKnownConcluded(startTimestamp, Consistency.LOCAL_READ) ||
         knownConcludedTransactions.isKnownConcluded(startTimestamp, Consistency.REMOTE_READ);
    }
}
