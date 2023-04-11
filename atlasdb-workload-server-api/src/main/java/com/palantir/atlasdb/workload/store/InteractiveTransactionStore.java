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

import com.palantir.atlasdb.workload.transaction.InteractiveTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.util.Optional;
import java.util.function.Consumer;

public interface InteractiveTransactionStore extends TransactionStore {
    /**
     * Provides a convenient interface for interacting with a transaction, and having each action automatically recorded.
     * @param interactiveTransactionConsumer Consumer which performs gets/puts/deletes
     * @return The witnessed transaction, if successfully committed.
     */
    Optional<WitnessedTransaction> readWrite(Consumer<InteractiveTransaction> interactiveTransactionConsumer);
}
