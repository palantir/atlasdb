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

public class DefaultKnownCommittedTransactions implements KnownCommittedTransactions {
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final KnownAbortedTransactions knownAbortedTransactions;

    public DefaultKnownCommittedTransactions(
            KnownConcludedTransactions knownConcludedTransactions, KnownAbortedTransactions knownAbortedTransactions) {
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.knownAbortedTransactions = knownAbortedTransactions;
    }

    @Override
    public boolean isKnownCommitted(long startTimestamp) {
        boolean concluded = knownConcludedTransactions.isKnownConcluded(startTimestamp);
        if (!concluded) {
            // todo(snanda): check in txn table - if there is a miss then we need to update
            //  knownConcludedTxns i.e. hit refresh on concludedTs
            return false;
        }
        return !knownAbortedTransactions.isKnownAborted(startTimestamp);
    }
}
