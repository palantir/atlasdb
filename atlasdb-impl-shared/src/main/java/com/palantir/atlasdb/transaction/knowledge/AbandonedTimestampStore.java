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

import java.util.Set;

public interface AbandonedTimestampStore {
    /**
     * Returns the start timestamps of the set of transactions in the provided range that are known to be unable to
     * commit. For timestamps in the range covered by a corresponding {@link KnownConcludedTransactionsStore}, this
     * should be accurate: transactions in the set definitely cannot commit, and transactions not in the set are
     * definitively committed. For timestamps not in the range covered by the corresponding
     * {@link KnownConcludedTransactionsStore}, transactions in the set certainly cannot commit, but transactions
     * not in the set may or may not be able to commit.
     */
    // TODO (jkong): Do we want to return a primitive typed Collection given how many numbers are floating around?
    Set<Long> getAbandonedTimestampsInRange(long startInclusive, long endInclusive);

    /**
     * Registers that the transaction with the provided start timestamp will not be able to commit.
     */
    void markAbandoned(long timestampToAbort);

    void markAbandoned(Set<Long> timestampsToAbort);
}
