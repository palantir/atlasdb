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

package com.palantir.atlasdb.transaction.api.expectations;

import com.palantir.atlasdb.transaction.api.Transaction;

/**
 * Implementors of this interface provide methods useful for tracking transactional expectations and whether
 * they were breached as well as relevant metrics and alerts. Transactional expectations represent transaction-level
 * limits and rules for proper usage of AtlasDB transactions (e.g. reading too much data overall).
 * Todo(aalouane): move this out of API once part 4 is merged
 */
public interface ExpectationsAwareTransaction extends Transaction {
    long getAgeMillis();

    /**
     * Returns a point-in-time snapshot of transaction read information.
     */
    TransactionReadInfo getReadInfo();

    /**
     * <<<<<<< HEAD
     * Update TEX data collection metrics for post-mortem transactions.
     * Invoke only after the transaction committed or aborted, and only once.
     * =======
     * Update TEX data collection metrics for (post-mortem) transactions.
     * Expected usage is that this method is called once after the transaction has been committed or aborted.
     * This method won't report metrics if called on an in-progress transaction.
     * Calling this twice after the transaction has committed or aborted will result in duplication.
     * Clients should not call this method directly.
     * >>>>>>> develop
     */
    void reportExpectationsCollectedData();
}
