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

package com.palantir.atlasdb.workload.transaction;

import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import java.util.List;
import java.util.Optional;

/**
 * Allows for convenient interaction with a transactional store.
 */
public interface InteractiveTransaction {
    Optional<Integer> read(String table, WorkloadCell workloadCell);

    void write(String table, WorkloadCell workloadCell, Integer value);

    void delete(String table, WorkloadCell workloadCell);

    List<WitnessedTransactionAction> witness();
}
