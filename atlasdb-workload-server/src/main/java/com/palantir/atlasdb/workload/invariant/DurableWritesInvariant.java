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

package com.palantir.atlasdb.workload.invariant;

import com.palantir.atlasdb.workload.store.InMemoryValidationStore;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.store.ValidationStore;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import io.vavr.Tuple;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public enum DurableWritesInvariant implements Invariant<Map<TableAndWorkloadCell, MismatchedValue>> {
    INSTANCE;

    @Override
    public void accept(
            WorkflowHistory workflowHistory, Consumer<Map<TableAndWorkloadCell, MismatchedValue>> invariantListener) {
        ValidationStore expectedState = InMemoryValidationStore.create(workflowHistory.history());
        ReadableTransactionStore storeToValidate = workflowHistory.transactionStore();
        Map<TableAndWorkloadCell, MismatchedValue> cellsThatDoNotMatch = expectedState
                .values()
                .map((writtenCell, expectedValue) -> {
                    Optional<MismatchedValue> maybeMismatchedValue = Optional.empty();
                    Optional<Integer> actualValue = storeToValidate.get(writtenCell.tableName(), writtenCell.cell());

                    if (!actualValue.equals(expectedValue)) {
                        maybeMismatchedValue = Optional.of(MismatchedValue.of(actualValue, expectedValue));
                    }

                    return Tuple.of(writtenCell, maybeMismatchedValue);
                })
                .filterValues(Optional::isPresent)
                .mapValues(Optional::get)
                .toJavaMap();
        invariantListener.accept(cellsThatDoNotMatch);
    }
}
