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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.common.concurrent.PTExecutors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Test;

public class DefaultWorkflowTest {
    private final TransactionStore store = mock(TransactionStore.class);

    private final KeyedTransactionTask<TransactionStore> transactionTask = mock(KeyedTransactionTask.class);

    private final ScheduledExecutorService scheduler = PTExecutors.newSingleThreadScheduledExecutor();
    private final ListeningExecutorService executionExecutor = MoreExecutors.listeningDecorator(scheduler);

    private final DefaultWorkflow<TransactionStore> workflow =
            DefaultWorkflow.create(store, transactionTask, createWorkflowConfiguration(2), executionExecutor);

    @Test
    public void handlesExceptionsInUnderlyingTasks() {
        RuntimeException transactionException = new RuntimeException("boo");
        when(transactionTask.apply(eq(store), anyInt())).thenThrow(transactionException);
        assertThatThrownBy(workflow::run)
                .hasMessage("Error when running workflow task")
                .hasCause(transactionException);
    }

    private WorkflowConfiguration createWorkflowConfiguration(int iterationCount) {
        return new WorkflowConfiguration() {
            @Override
            public int iterationCount() {
                return iterationCount;
            }
        };
    }
}
