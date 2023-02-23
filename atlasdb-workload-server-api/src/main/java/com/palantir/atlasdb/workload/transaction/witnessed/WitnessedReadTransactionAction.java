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

package com.palantir.atlasdb.workload.transaction.witnessed;

import com.palantir.atlasdb.workload.store.WorkloadCell;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable(builder = false)
public interface WitnessedReadTransactionAction extends WitnessedTransactionAction {

    @Override
    @Value.Parameter
    String table();

    @Override
    @Value.Parameter
    WorkloadCell cell();

    /** Value of the cell from the row read. Empty if it does not exist. */
    @Value.Parameter
    Optional<Integer> value();

    @Override
    default <T> T accept(WitnessedTransactionActionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    static WitnessedReadTransactionAction of(String table, WorkloadCell cell, Optional<Integer> value) {
        return ImmutableWitnessedReadTransactionAction.of(table, cell, value);
    }
}
