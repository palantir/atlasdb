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

import com.palantir.atlasdb.workload.transaction.InMemoryTransactionReplayer;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import io.vavr.collection.Map;
import java.util.List;
import java.util.Optional;

public final class InMemoryValidationStore implements ValidationStore {

    private final Map<TableAndWorkloadCell, Optional<Integer>> values;

    private InMemoryValidationStore(Map<TableAndWorkloadCell, Optional<Integer>> values) {
        this.values = values;
    }

    public static InMemoryValidationStore create(List<WitnessedTransaction> history) {
        InMemoryTransactionReplayer replayer = new InMemoryTransactionReplayer();
        history.stream()
                .map(WitnessedTransaction::actions)
                .forEach(witnessedTransactionActions ->
                        witnessedTransactionActions.forEach(action -> action.accept(replayer)));
        return new InMemoryValidationStore(replayer.getValues());
    }

    @Override
    public Map<TableAndWorkloadCell, Optional<Integer>> values() {
        return values;
    }
}
