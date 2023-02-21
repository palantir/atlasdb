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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import java.util.function.Consumer;

public class LongLivedRowsWorkflow implements Workflow {
    private final AtlasDbTransactionStore atlasDbTransactionStore;
    private final WorkflowConfiguration workflowConfiguration;

    public LongLivedRowsWorkflow(
            AtlasDbTransactionStore atlasDbTransactionStore,
            WorkflowConfiguration workflowConfiguration) {
        this.atlasDbTransactionStore = atlasDbTransactionStore;
        this.workflowConfiguration = workflowConfiguration;
    }

    @Override
    public void run() {
        for (int iteration = 0; iteration < workflowConfiguration.iterationCount(); iteration++) {
            workflowConfiguration.transactionExecutor().execute(() -> {
                atlasDbTransactionStore.readWrite(ImmutableList.of(
                ));
            });
        }
    }

    @Override
    public Workflow onComplete(Consumer<WorkflowHistory> consumer) {
        return null;
    }
}
