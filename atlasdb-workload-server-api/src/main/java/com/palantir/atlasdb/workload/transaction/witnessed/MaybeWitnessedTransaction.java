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

import com.palantir.logsafe.Preconditions;
import org.immutables.value.Value;

/**
 * For transactions that we witnessed, but are in a state which we are
 * unsure if they have committed or not due to failures.
 */
@Value.Immutable
public interface MaybeWitnessedTransaction extends WitnessedTransaction {
    @Value.Check
    default void check() {
        Preconditions.checkArgument(
                commitTimestamp().isPresent(),
                "Given how the transaction protocol works, a transaction for which we haven't retrieved the commit "
                        + "timestamp has not written anything of significance to the database");
    }

    @Override
    default <T> T accept(WitnessedTransactionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    static ImmutableMaybeWitnessedTransaction.Builder builder() {
        return ImmutableMaybeWitnessedTransaction.builder();
    }

    default FullyWitnessedTransaction toFullyWitnessed() {
        return FullyWitnessedTransaction.builder()
                .startTimestamp(startTimestamp())
                .commitTimestamp(commitTimestamp())
                .actions(actions())
                .build();
    }
}
