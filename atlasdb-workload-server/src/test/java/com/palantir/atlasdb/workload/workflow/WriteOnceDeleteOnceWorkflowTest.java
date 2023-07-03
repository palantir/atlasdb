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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.buggify.AlwaysBuggifyFactory;
import com.palantir.atlasdb.buggify.api.BuggifyFactory;
import com.palantir.atlasdb.buggify.impl.DefaultBuggify;
import com.palantir.atlasdb.buggify.impl.NoOpBuggify;
import com.palantir.atlasdb.buggify.impl.NoOpBuggifyFactory;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;
import org.junit.Test;

public class WriteOnceDeleteOnceWorkflowTest {
    private static final String TABLE_NAME = "my.latte";
    private static final WriteOnceDeleteOnceWorkflowConfiguration CONFIGURATION =
            ImmutableWriteOnceDeleteOnceWorkflowConfiguration.builder()
                    .tableConfiguration(ImmutableTableConfiguration.builder()
                            .tableName(TABLE_NAME)
                            .isolationLevel(IsolationLevel.SERIALIZABLE)
                            .build())
                    .rateLimit(Double.MAX_VALUE)
                    .iterationCount(1)
                    .build();

    private final InteractiveTransactionStore memoryStore = AtlasDbTransactionStore.create(
            TransactionManagers.createInMemory(ImmutableSet.of()),
            ImmutableMap.of(
                    TableReference.createWithEmptyNamespace(TABLE_NAME),
                    AtlasDbUtils.tableMetadata(IsolationLevel.SERIALIZABLE)));

    @Test
    public void deleteWhenBuggifyTrue() {
        Workflow workflow = createWorkflow(AlwaysBuggifyFactory.INSTANCE, i -> i);
        WorkflowHistory history = workflow.run();
        WitnessedTransaction transaction = Iterables.getOnlyElement(history.history());
        WitnessedTransactionAction action = Iterables.getOnlyElement(transaction.actions());
        assertThat(action).isInstanceOf(WitnessedDeleteTransactionAction.class);
    }

    @Test
    public void writeWhenBuggifyFalse() {
        Workflow workflow = createWorkflow(NoOpBuggifyFactory.INSTANCE, i -> i);
        WorkflowHistory history = workflow.run();
        WitnessedTransaction transaction = Iterables.getOnlyElement(history.history());
        WitnessedTransactionAction action = Iterables.getOnlyElement(transaction.actions());
        assertThat(action).isInstanceOf(WitnessedWriteTransactionAction.class);
    }

    @Test
    public void calculateChanceForDeleteHasExpectedValues() {
        assertThat(WriteOnceDeleteOnceWorkflows.calculateChanceForDelete(0, 100))
                .isEqualTo(0.01);
        assertThat(WriteOnceDeleteOnceWorkflows.calculateChanceForDelete(99, 100))
                .isEqualTo(1.00);
    }

    @Test
    public void writesThenDeletes() {
        Iterator<Integer> keys = List.of(0, 0, 1, 1).iterator();
        IntFunction<Integer> rowKeyGenerator = _ignore -> keys.next();
        BuggifyFactory alternatingBuggify = mock(BuggifyFactory.class);
        when(alternatingBuggify.maybe(anyDouble()))
                .thenReturn(
                        NoOpBuggify.INSTANCE, DefaultBuggify.INSTANCE, NoOpBuggify.INSTANCE, DefaultBuggify.INSTANCE);
        Workflow workflow = createWorkflow(alternatingBuggify, rowKeyGenerator, 4);

        List<WitnessedTransaction> transactions = workflow.run().history();

        assertThat(transactions.size()).isEqualTo(4);
        assertThat(getTransactionAction(transactions.get(0), WitnessedWriteTransactionAction.class)
                        .cell()
                        .key())
                .isEqualTo(0);
        assertThat(getTransactionAction(transactions.get(1), WitnessedDeleteTransactionAction.class)
                        .cell()
                        .key())
                .isEqualTo(0);
        assertThat(getTransactionAction(transactions.get(2), WitnessedWriteTransactionAction.class)
                        .cell()
                        .key())
                .isEqualTo(1);
        assertThat(getTransactionAction(transactions.get(3), WitnessedDeleteTransactionAction.class)
                        .cell()
                        .key())
                .isEqualTo(1);
    }

    private <T> T getTransactionAction(WitnessedTransaction transaction, Class<T> clazz) {
        WitnessedTransactionAction action = Iterables.getOnlyElement(transaction.actions());
        assertThat(action).isInstanceOf(clazz);
        return (T) action;
    }

    private Workflow createWorkflow(BuggifyFactory factory, IntFunction<Integer> randomNumberGenerator) {
        return createWorkflow(factory, randomNumberGenerator, CONFIGURATION.iterationCount());
    }

    private Workflow createWorkflow(
            BuggifyFactory factory, IntFunction<Integer> randomNumberGenerator, int iterationCount) {
        return WriteOnceDeleteOnceWorkflows.create(
                memoryStore,
                ImmutableWriteOnceDeleteOnceWorkflowConfiguration.builder()
                        .from(CONFIGURATION)
                        .iterationCount(iterationCount)
                        .build(),
                MoreExecutors.newDirectExecutorService(),
                factory,
                randomNumberGenerator);
    }
}
