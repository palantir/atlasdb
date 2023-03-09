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

package com.palantir.atlasdb.workload.server;

import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.workload.config.WorkloadServerInstallConfig;
import com.palantir.atlasdb.workload.config.WorkloadServerRuntimeConfig;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.atlasdb.workload.workflow.SingleRowTwoCellsWorkflows;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.refreshable.Refreshable;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class WorkloadServer {

    private final ExecutorService executorService;
    private final WorkloadServerInstallConfig installConfig;
    private final Refreshable<WorkloadServerRuntimeConfig> runtimeConfig;
    private final UserAgent userAgent;
    private final MetricsManager metricsManager;

    public WorkloadServer(
            WorkloadServerInstallConfig installConfig,
            Refreshable<WorkloadServerRuntimeConfig> runtimeConfig,
            ExecutorService executorService,
            UserAgent userAgent,
            MetricsManager metricsManager) {
        this.executorService = executorService;
        this.installConfig = installConfig;
        this.runtimeConfig = runtimeConfig;
        this.userAgent = userAgent;
        this.metricsManager = metricsManager;
    }

    public void run() {
        TransactionManager transactionManager = TransactionManagers.builder()
                .config(installConfig.atlas())
                .userAgent(userAgent)
                .globalMetricsRegistry(metricsManager.getRegistry())
                .globalTaggedMetricRegistry(metricsManager.getTaggedRegistry())
                .runtimeConfigSupplier(runtimeConfig.map(WorkloadServerRuntimeConfig::atlas))
                .build()
                .serializable();

        TableReference tableReference = installConfig
                .atlas()
                .namespace()
                .map(namespace -> TableReference.create(
                        Namespace.create(namespace),
                        installConfig.singleCellWorkflowConfiguration().tableName()))
                .orElseGet(() -> TableReference.createWithEmptyNamespace(
                        installConfig.singleCellWorkflowConfiguration().tableName()));

        AtlasDbTransactionStore store = AtlasDbTransactionStore.create(
                transactionManager, Map.of(tableReference, AtlasDbUtils.tableMetadata(ConflictHandler.SERIALIZABLE)));
        Workflow workflow = SingleRowTwoCellsWorkflows.createSingleRowTwoCell(
                store, installConfig.singleCellWorkflowConfiguration());
        workflow.run();
    }
}
