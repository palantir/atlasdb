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
import com.palantir.atlasdb.buggify.impl.DefaultBuggifyFactory;
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.atlasdb.workload.background.BackgroundCassandraJob;
import com.palantir.atlasdb.workload.config.WorkloadServerConfiguration;
import com.palantir.atlasdb.workload.logging.LoggingUtils;
import com.palantir.atlasdb.workload.resource.AntithesisCassandraSidecarResource;
import com.palantir.atlasdb.workload.runner.AntithesisWorkflowValidatorRunner;
import com.palantir.atlasdb.workload.runner.DefaultWorkflowRunner;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStoreFactory;
import com.palantir.atlasdb.workload.workflow.SingleRowTwoCellsWorkflows;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowAndInvariants;
import com.palantir.atlasdb.workload.workflow.WorkflowFactory;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgent.Agent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WorkloadServerLauncher extends Application<WorkloadServerConfiguration> {
    private static final SafeLogger log = SafeLoggerFactory.get(WorkloadServerLauncher.class);
    private static final UserAgent USER_AGENT = UserAgent.of(Agent.of("AtlasDbWorkloadServer", "0.0.0"));
    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();

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

        // Important to make sure we can control when Antithesis fuzzer kicks in. We need to make sure it's logged
        // before we actually try to choose the workflows to be run.
        LoggingUtils.setSynchronousLogging();

        scheduleBackgroundJobs(environment);

        ExecutorService workflowRunnerExecutor =
                environment.lifecycle().executorService("workflow-runner").build();
        workflowRunnerExecutor.execute(() -> runWorkflows(configuration, environment));
    }

    private void scheduleBackgroundJobs(Environment environment) {
        ScheduledExecutorService backgroundJobExecutor = environment
                .lifecycle()
                .scheduledExecutorService("background-job")
                .build();
        backgroundJobExecutor.scheduleAtFixedRate(
                new BackgroundCassandraJob(
                        List.of("cassandra1", "cassandra2", "cassandra3"),
                        AntithesisCassandraSidecarResource.INSTANCE,
                        DefaultBuggifyFactory.INSTANCE),
                0,
                2,
                TimeUnit.SECONDS);
    }

    private void runWorkflows(WorkloadServerConfiguration configuration, Environment environment) {
        // This is a single threaded executor; this is intentional, so that we only run one workflow at a time.
        ExecutorService antithesisWorkflowRunnerExecutorService = environment
                .lifecycle()
                .executorService(SingleRowTwoCellsWorkflows.class.getSimpleName())
                .build();

        MetricsManager metricsManager = MetricsManagers.of(environment.metrics(), taggedMetricRegistry);
        AtlasDbTransactionStoreFactory transactionStoreFactory = AtlasDbTransactionStoreFactory.createFromConfig(
                configuration.install().atlas(),
                Refreshable.only(configuration.runtime().atlas()),
                USER_AGENT,
                metricsManager);

        WorkflowFactory workflowFactory =
                new WorkflowFactory(taggedMetricRegistry, new DefaultWorkflowExecutorFactory(environment.lifecycle()));

        AntithesisWorkflowValidatorRunner.create(new DefaultWorkflowRunner(
                        MoreExecutors.listeningDecorator(antithesisWorkflowRunnerExecutorService)))
                .run(() -> selectWorkflowsToRun(
                        configuration,
                        // We intentionally add randomness when creating the workflows (e.g., the executor pool size)
                        // and so we must create the workflows under the fuzzer
                        workflowFactory.createAllWorkflowsAndInvariants(
                                configuration.install(), transactionStoreFactory)));

        log.info("Finished running desired workflows successfully");
        log.info("antithesis: terminate");

        workflowsRanLatch.countDown();

        if (configuration.install().exitAfterRunning()) {
            System.exit(0);
        }
    }

    private static List<WorkflowAndInvariants<Workflow>> selectWorkflowsToRun(
            WorkloadServerConfiguration configuration, List<WorkflowAndInvariants<Workflow>> workflowsAndInvariants) {
        Collections.shuffle(workflowsAndInvariants, SECURE_RANDOM);
        switch (configuration.install().workflowExecutionConfig().runMode()) {
            case ONE:
                return workflowsAndInvariants.subList(0, 1);
            case ALL:
                return workflowsAndInvariants;
            default:
                throw new SafeIllegalStateException(
                        "Unexpected run mode",
                        SafeArg.of(
                                "runMode",
                                configuration
                                        .install()
                                        .workflowExecutionConfig()
                                        .runMode()));
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
