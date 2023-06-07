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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedSingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.common.concurrent.PTExecutors;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RandomWorkflowsTest {
    private static final String TABLE_NAME = "random.random";
    private static final RandomWorkflowConfiguration CONFIGURATION = ImmutableRandomWorkflowConfiguration.builder()
            .tableConfiguration(ImmutableTableConfiguration.builder()
                    .tableName(TABLE_NAME)
                    .isolationLevel(IsolationLevel.SERIALIZABLE)
                    .build())
            .iterationCount(1)
            .build();

    private final TransactionStore memoryStore = AtlasDbTransactionStore.create(
            TransactionManagers.createInMemory(ImmutableSet.of()),
            ImmutableMap.of(
                    TableReference.createWithEmptyNamespace(TABLE_NAME),
                    AtlasDbUtils.tableMetadata(IsolationLevel.SERIALIZABLE)));

    @Mock
    private SecureRandom random;

    private Workflow workflow;

    @Before
    public void before() {
        this.workflow = RandomWorkflows.create(
                memoryStore,
                CONFIGURATION,
                MoreExecutors.listeningDecorator(PTExecutors.newSingleThreadExecutor()),
                random);
    }

    @Test
    public void workflowHistoryTransactionStoreShouldBeReadOnly() {
        WorkflowHistory history = workflow.run();
        assertThat(history.transactionStore())
                .as("should return a read only transaction store")
                .isInstanceOf(ReadOnlyTransactionStore.class);
    }

    @Test
    public void workflowRunsReadWritesDeletes() {
        when(random.nextInt(anyInt())).thenReturn(1);
        WorkflowHistory history = workflow.run();
        List<WitnessedTransactionAction> actions = history.history().stream()
                .map(WitnessedTransaction::actions)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        assertThat(actions).anyMatch(WitnessedWriteTransactionAction.class::isInstance);
        assertThat(actions).anyMatch(WitnessedSingleCellReadTransactionAction.class::isInstance);
        assertThat(actions).anyMatch(WitnessedDeleteTransactionAction.class::isInstance);
        assertThat(actions).anyMatch(WitnessedRowColumnRangeReadTransactionAction.class::isInstance);
    }
}
