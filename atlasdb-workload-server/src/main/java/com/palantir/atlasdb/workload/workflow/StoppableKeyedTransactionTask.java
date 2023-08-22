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

package com.palantir.atlasdb.workload.workflow;

import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A keyed transaction task which will stop running if it encounters a failure. Implementing classes must record the
 * failure themselves, and should not throw an exception.
 */
public abstract class StoppableKeyedTransactionTask<T extends TransactionStore, ViolationT>
        implements KeyedTransactionTask<T> {

    private final AtomicBoolean stopRunning;
    private final Consumer<ViolationT> onFailure;

    public StoppableKeyedTransactionTask(AtomicBoolean stopRunning, Consumer<ViolationT> onFailure) {
        this.stopRunning = stopRunning;
        this.onFailure = onFailure;
    }

    protected final void recordFailure(ViolationT violation) {
        if (stopRunning.compareAndSet(false, true)) {
            onFailure.accept(violation);
        }
    }

    protected abstract Optional<WitnessedTransaction> run(T store, Integer index);

    @Override
    public final Optional<WitnessedTransaction> apply(T store, Integer index) {
        if (stopRunning.get()) {
            return Optional.empty();
        }

        return run(store, index);
    }
}
