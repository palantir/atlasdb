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

package com.palantir.atlasdb.workload;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.atlasdb.workload.config.WorkloadServerConfiguration;
import com.palantir.atlasdb.workload.invariant.DurableWritesInvariantMetricReporter;
import com.palantir.atlasdb.workload.invariant.SerializableInvariantLogReporter;
import com.palantir.atlasdb.workload.runner.AntithesisWorkflowRunner;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStoreFactory;
import com.palantir.atlasdb.workload.workflow.SingleRowTwoCellsWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.SingleRowTwoCellsWorkflows;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgent.Agent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import io.dropwizard.Application;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class WorkloadServerLauncher extends Application<WorkloadServerConfiguration> {

    private static final SafeLogger log = SafeLoggerFactory.get(WorkloadServerLauncher.class);
    private static final UserAgent USER_AGENT = UserAgent.of(Agent.of("AtlasDbWorkloadServer", "0.0.0"));

    private final TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
    private final CountDownLatch workflowsRanLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        new WorkloadServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<WorkloadServerConfiguration> bootstrap) {
        MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate("AtlasDbWorkLoadServer");
        bootstrap.setMetricRegistry(metricRegistry);
        bootstrap.setObjectMapper(ObjectMappers.newServerObjectMapper());
        bootstrap.getObjectMapper().setSubtypeResolver(new DiscoverableSubtypeResolver());
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        super.initialize(bootstrap);
    }

    @Override
    public void run(WorkloadServerConfiguration configuration, Environment environment) {
        environment.getObjectMapper().registerModule(new Jdk8Module()).registerModule(new JavaTimeModule());

        ExecutorService workflowRunnerExecutor =
                environment.lifecycle().executorService("workflow-runner").build();

        workflowRunnerExecutor.execute(() -> runWorkflows(configuration, environment));
    }

    private void runWorkflows(WorkloadServerConfiguration configuration, Environment environment) {
        ExecutorService singleRowTwoCellsExecutorService = environment
                .lifecycle()
                .executorService(SingleRowTwoCellsWorkflows.class.getSimpleName())
                .minThreads(100)
                .maxThreads(1024)
                .build();
        MetricsManager metricsManager = MetricsManagers.of(environment.metrics(), taggedMetricRegistry);
        AtlasDbTransactionStoreFactory transactionStoreFactory = AtlasDbTransactionStoreFactory.createFromConfig(
                configuration.install().atlas(),
                Refreshable.only(configuration.runtime().atlas()),
                USER_AGENT,
                metricsManager);
        SingleRowTwoCellsWorkflowConfiguration workflowConfig =
                configuration.install().singleRowTwoCellsConfig();

        log.info("antithesis: start_faults");
        AntithesisWorkflowRunner.INSTANCE.run(
                SingleRowTwoCellsWorkflows.createSingleRowTwoCell(
                        transactionStoreFactory.create(
                                Map.of(
                                        workflowConfig.tableConfiguration().tableName(),
                                        workflowConfig.tableConfiguration().isolationLevel()),
                                Set.of()),
                        workflowConfig,
                        MoreExecutors.listeningDecorator(singleRowTwoCellsExecutorService)),
                List.of(
                        new DurableWritesInvariantMetricReporter(
                                SingleRowTwoCellsWorkflows.class.getSimpleName(),
                                DurableWritesMetrics.of(taggedMetricRegistry)),
                        SerializableInvariantLogReporter.INSTANCE));
        log.info("antithesis: terminate");

        workflowsRanLatch.countDown();

        if (configuration.install().exitAfterRunning()) {
            System.exit(0);
        }
    }

    @VisibleForTesting
    CountDownLatch workflowsRanLatch() {
        return workflowsRanLatch;
    }

    @VisibleForTesting
    TaggedMetricRegistry getTaggedMetricRegistry() {
        return taggedMetricRegistry;
    }
}
