/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.workload.transaction.TransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.util.List;
import java.util.Optional;

/**
 * A transactional store for the workload server to read/write from.
 */
public interface TransactionStore {
    /**
     * Perform a read for a given row and return a cell if it exists.
     * Ideally this endpoint is only used for verification purposes, as it does not return a witnessed transaction.
     *
     * @param table Table to read from
     * @param cell Cell to read from
     * @return The value of the cell for a given table.
     */
    Optional<Integer> get(String table, WorkloadCell cell);

    /**
     * Performs a list of transaction actions in order, and returns the transaction executed if it committed
     * successfully.
     *
     * @param actions List of transaction actions (read/write/delete) to be executed in order.
     * @return The witnessed transaction, if successfully committed.
     */
    Optional<WitnessedTransaction> readWrite(List<TransactionAction> actions);
}
