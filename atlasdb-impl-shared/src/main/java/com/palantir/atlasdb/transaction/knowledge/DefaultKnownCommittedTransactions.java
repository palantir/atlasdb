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
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
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
        if (isConcludedLocal(startTimestamp)) {
            return true;
        }

        Optional<Long> commitTs = Optional.ofNullable(transactionService.get(startTimestamp));
        /**
         * We have not wired in the transactionService#safeGet() api. Broad idea will be:
         * case committed ->  return true, in this case we do not really care about refreshing the concluded store as
         * the remote call might be a waste.
         * case inProgress -> return false.
         * case deleted -> refresh concluded store because in order to delete the transaction would have to be
         * first declared as concluded; if the remote call indicated that the transaction is not concluded then throw.
         * */

        // This call also refreshed the concluded data store.
        boolean concludedRemote = isConcludedRemote(startTimestamp);

        if (!concludedRemote) {
            throw new SafeIllegalStateException("The startTs has been deleted but somehow concludedStore has not "
                    + "progressed. This can be indicative of SEVERE DATA CORRUPTION!");
        }

        return false;
    }

    private boolean isConcludedLocal(long startTimestamp) {
        return knownConcludedTransactions.isKnownConcluded(startTimestamp, Consistency.LOCAL_READ);
    }

    private boolean isConcludedRemote(long startTimestamp) {
        return knownConcludedTransactions.isKnownConcluded(startTimestamp, Consistency.REMOTE_READ);
    }
}
