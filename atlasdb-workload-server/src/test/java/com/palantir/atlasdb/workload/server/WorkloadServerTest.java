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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.atlasdb.workload.SingleRowTwoCellsWorkflowMetrics;
import com.palantir.atlasdb.workload.config.ImmutableWorkloadServerInstallConfig;
import com.palantir.atlasdb.workload.config.ImmutableWorkloadServerRuntimeConfig;
import com.palantir.atlasdb.workload.config.WorkloadServerInstallConfig;
import com.palantir.atlasdb.workload.config.WorkloadServerRuntimeConfig;
import com.palantir.atlasdb.workload.workflow.ImmutableSingleCellWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.ImmutableWorkflowConfiguration;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.refreshable.Refreshable;
import java.util.concurrent.Executors;
import org.junit.Test;

public class WorkloadServerTest {

    private static final WorkloadServerInstallConfig WORKLOAD_SERVER_INSTALL_CONFIG =
            ImmutableWorkloadServerInstallConfig.builder()
                    .atlas(ImmutableAtlasDbConfig.builder()
                            .keyValueService(new InMemoryAtlasDbConfig())
                            .build())
                    .singleCellWorkflowConfiguration(ImmutableSingleCellWorkflowConfiguration.builder()
                            .tableName("foo")
                            .genericWorkflowConfiguration(ImmutableWorkflowConfiguration.builder()
                                    .iterationCount(1000)
                                    .executionExecutor(
                                            MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(100)))
                                    .build())
                            .build())
                    .build();

    private static final WorkloadServerRuntimeConfig WORKLOAD_SERVER_RUNTIME_CONFIG =
            ImmutableWorkloadServerRuntimeConfig.builder()
                    .atlas(AtlasDbRuntimeConfig.defaultRuntimeConfig())
                    .build();

    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    @Test
    public void runExecutesWorkflows() {
        WorkloadServer workloadServer = new WorkloadServer(
                WORKLOAD_SERVER_INSTALL_CONFIG,
                Refreshable.only(WORKLOAD_SERVER_RUNTIME_CONFIG),
                Executors.newCachedThreadPool(),
                UserAgent.of(UserAgent.Agent.of("WorkloadServer", "0.0.0")),
                metricsManager);
        workloadServer.run();
        SingleRowTwoCellsWorkflowMetrics workflowMetrics = SingleRowTwoCellsWorkflowMetrics.of(metricsManager.getTaggedRegistry());
        assertThat(workflowMetrics.numberOfCellsWritten().workflow())
    }
}
