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
import java.util.function.BiFunction;

/**
 * A task that performs some operations on a {@link TransactionStore}, possibly using the index of the task
 * as an input, and returns an {@link Optional<WitnessedTransaction>}. Here, "index" refers to an integer between 0
 * and the {@link WorkflowConfiguration#iterationCount()} - each copy of the task will receive a different integer.
 */
@FunctionalInterface
public interface IndexedTransactionTask extends BiFunction<TransactionStore, Integer, Optional<WitnessedTransaction>> {}
