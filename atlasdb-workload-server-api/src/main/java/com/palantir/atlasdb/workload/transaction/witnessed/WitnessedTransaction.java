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

import com.palantir.atlasdb.workload.transaction.TransactionAction;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface WitnessedTransaction {
    /** Start timestamp of the transaction. */
    long startTimestamp();

    /** Commit timestamp of the transaction. */
    long commitTimestamp();

    /** Provides an in-order list of actions that were performed during the transaction's execution. */
    List<TransactionAction> actions();
}
