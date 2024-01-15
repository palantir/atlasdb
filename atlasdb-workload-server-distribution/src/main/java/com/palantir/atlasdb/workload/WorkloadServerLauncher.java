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
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.buggify.impl.DefaultBuggifyFactory;
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.atlasdb.workload.background.BackgroundCassandraJob;
import com.palantir.atlasdb.workload.config.WorkloadServerConfiguration;
import com.palantir.atlasdb.workload.invariant.DurableWritesInvariantMetricReporter;
import com.palantir.atlasdb.workload.invariant.SerializableInvariantLogReporter;
import com.palantir.atlasdb.workload.resource.AntithesisCassandraSidecarResource;
import com.palantir.atlasdb.workload.runner.AntithesisWorkflowValidatorRunner;
import com.palantir.atlasdb.workload.runner.DefaultWorkflowRunner;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStoreFactory;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.workflow.RandomWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.RandomWorkflows;
import com.palantir.atlasdb.workload.workflow.SingleBusyCellReadNoTouchWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.SingleBusyCellReadNoTouchWorkflows;
import com.palantir.atlasdb.workload.workflow.SingleBusyCellWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.SingleBusyCellWorkflows;
import com.palantir.atlasdb.workload.workflow.SingleRowTwoCellsWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.SingleRowTwoCellsWorkflows;
import com.palantir.atlasdb.workload.workflow.TransientRowsWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.TransientRowsWorkflows;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowAndInvariants;
import com.palantir.atlasdb.workload.workflow.WorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.WriteOnceDeleteOnceWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.WriteOnceDeleteOnceWorkflows;
import com.palantir.atlasdb.workload.workflow.bank.BankBalanceWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.bank.BankBalanceWorkflows;
import com.palantir.atlasdb.workload.workflow.ring.RingWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.ring.RingWorkflows;
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
import io.dropwizard.Application;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
        SingleRowTwoCellsWorkflowConfiguration singleRowTwoCellsConfig =
                configuration.install().singleRowTwoCellsConfig();
        RingWorkflowConfiguration ringWorkflowConfiguration =
                configuration.install().ringConfig();
        TransientRowsWorkflowConfiguration transientRowsWorkflowConfiguration =
                configuration.install().transientRowsConfig();
        SingleBusyCellWorkflowConfiguration singleBusyCellWorkflowConfiguration =
                configuration.install().singleBusyCellConfig();
        SingleBusyCellReadNoTouchWorkflowConfiguration singleBusyCellReadNoTouchWorkflowConfiguration =
                configuration.install().singleBusyCellReadsNoTouchConfig();
        BankBalanceWorkflowConfiguration bankBalanceConfig =
                configuration.install().bankBalanceConfig();
        RandomWorkflowConfiguration randomWorkflowConfig =
                configuration.install().randomConfig();
        WriteOnceDeleteOnceWorkflowConfiguration writeOnceDeleteOnceConfig =
                configuration.install().writeOnceDeleteOnceConfig();

        waitForTransactionStoreFactoryToBeInitialized(transactionStoreFactory);

        List<WorkflowAndInvariants<Workflow>> workflowsAndInvariants = new ArrayList<>(List.of(
                createSingleRowTwoCellsWorkflowValidator(
                        transactionStoreFactory, singleRowTwoCellsConfig, environment.lifecycle()),
                createRingWorkflowValidator(
                        transactionStoreFactory, ringWorkflowConfiguration, environment.lifecycle()),
                createTransientRowsWorkflowValidator(
                        transactionStoreFactory, transientRowsWorkflowConfiguration, environment.lifecycle()),
                createSingleBusyCellWorkflowValidator(
                        transactionStoreFactory, singleBusyCellWorkflowConfiguration, environment.lifecycle()),
                createSingleBusyCellReadNoTouchWorkflowValidator(
                        transactionStoreFactory,
                        singleBusyCellReadNoTouchWorkflowConfiguration,
                        environment.lifecycle()),
                createBankBalanceWorkflow(transactionStoreFactory, bankBalanceConfig, environment.lifecycle()),
                createRandomWorkflow(transactionStoreFactory, randomWorkflowConfig, environment.lifecycle()),
                createWriteOnceDeleteOnceWorkflow(
                        transactionStoreFactory, writeOnceDeleteOnceConfig, environment.lifecycle())));
        Collections.shuffle(workflowsAndInvariants, DefaultNativeSamplingSecureRandomFactory.INSTANCE.create());

        List<WorkflowAndInvariants<Workflow>> workflowsAndInvariantsToRun;
        switch (configuration.install().workflowExecutionConfig().runMode()) {
            case ONE:
                workflowsAndInvariantsToRun = workflowsAndInvariants.subList(0, 1);
                break;
            case ALL:
                workflowsAndInvariantsToRun = workflowsAndInvariants;
                break;
            default:
                throw new SafeIllegalStateException("Unexpected run mode",
                        SafeArg.of("runMode", configuration.install().workflowExecutionConfig().runMode()));
        }

        new AntithesisWorkflowValidatorRunner(new DefaultWorkflowRunner(
                MoreExecutors.listeningDecorator(antithesisWorkflowRunnerExecutorService)))
                .run(workflowsAndInvariantsToRun);

        log.info("antithesis: terminate");

        workflowsRanLatch.countDown();

        if (configuration.install().exitAfterRunning()) {
            System.exit(0);
        }
    }

    private void waitForTransactionStoreFactoryToBeInitialized(AtlasDbTransactionStoreFactory factory) {
        // TODO (jkong): This is awful, but sufficient for now.
        Instant deadline = Instant.now().plusSeconds(TimeUnit.MINUTES.toSeconds(5));
        while (Instant.now().isBefore(deadline)) {
            if (factory.isInitialized()) {
                log.info("AtlasDB transaction store factory initialized.");
                return;
            } else {
                log.info(
                        "AtlasDB transaction store factory not yet initialized. Waiting for five seconds; we won't"
                                + " retry after {}.",
                        SafeArg.of("deadline", deadline));
                Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(5));
            }
        }
        log.error("AtlasDB transaction store factory not initialized after five minutes, which suggests that there's"
                + " likely to be some issue with starting up one of our service's dependencies.");
        log.info("antithesis: terminate");
        log.error("Workflow will now exit.");
        System.exit(1);
    }

    private WorkflowAndInvariants<Workflow> createTransientRowsWorkflowValidator(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            TransientRowsWorkflowConfiguration workflowConfig,
            LifecycleEnvironment lifecycle) {
        ExecutorService executorService =
                createExecutorService(workflowConfig, lifecycle, TransientRowsWorkflows.class);
        InteractiveTransactionStore transactionStore =
                transactionStoreFactory.create(workflowConfig.tableConfiguration());
        return WorkflowAndInvariants.builder()
                .workflow(TransientRowsWorkflows.create(
                        transactionStore, workflowConfig, MoreExecutors.listeningDecorator(executorService)))
                .addInvariantReporters(new DurableWritesInvariantMetricReporter(
                        TransientRowsWorkflows.class.getSimpleName(), DurableWritesMetrics.of(taggedMetricRegistry)))
                .addInvariantReporters(SerializableInvariantLogReporter.INSTANCE)
                .addInvariantReporters(TransientRowsWorkflows.getSummaryLogInvariantReporter(workflowConfig))
                .build();
    }

    private WorkflowAndInvariants<Workflow> createSingleBusyCellWorkflowValidator(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            SingleBusyCellWorkflowConfiguration workflowConfig,
            LifecycleEnvironment lifecycle) {
        ExecutorService readExecutor = lifecycle
                .executorService(SingleBusyCellWorkflowConfiguration.class.getSimpleName() + "-read")
                .minThreads(workflowConfig.maxThreadCount() / 2)
                .maxThreads(workflowConfig.maxThreadCount() / 2)
                .build();
        ExecutorService writeExecutor = lifecycle
                .executorService(SingleBusyCellWorkflowConfiguration.class.getSimpleName() + "-write")
                .minThreads(workflowConfig.maxThreadCount() / 2)
                .maxThreads(workflowConfig.maxThreadCount() / 2)
                .build();
        InteractiveTransactionStore transactionStore =
                transactionStoreFactory.create(workflowConfig.tableConfiguration());
        return WorkflowAndInvariants.of(
                SingleBusyCellWorkflows.create(
                        transactionStore,
                        workflowConfig,
                        MoreExecutors.listeningDecorator(readExecutor),
                        MoreExecutors.listeningDecorator(writeExecutor)),
                new DurableWritesInvariantMetricReporter(
                        SingleBusyCellWorkflows.class.getSimpleName(), DurableWritesMetrics.of(taggedMetricRegistry)),
                SerializableInvariantLogReporter.INSTANCE);
    }

    private WorkflowAndInvariants<Workflow> createSingleBusyCellReadNoTouchWorkflowValidator(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            SingleBusyCellReadNoTouchWorkflowConfiguration workflowConfig,
            LifecycleEnvironment lifecycle) {
        ExecutorService readExecutor = lifecycle
                .executorService(SingleBusyCellReadNoTouchWorkflowConfiguration.class.getSimpleName() + "-read")
                .minThreads(workflowConfig.maxThreadCount() / 2)
                .maxThreads(workflowConfig.maxThreadCount() / 2)
                .build();
        ExecutorService writeExecutor = lifecycle
                .executorService(SingleBusyCellReadNoTouchWorkflowConfiguration.class.getSimpleName() + "-write")
                .minThreads(workflowConfig.maxThreadCount() / 2)
                .maxThreads(workflowConfig.maxThreadCount() / 2)
                .build();
        InteractiveTransactionStore transactionStore =
                transactionStoreFactory.create(workflowConfig.tableConfiguration());
        return WorkflowAndInvariants.of(
                SingleBusyCellReadNoTouchWorkflows.create(
                        transactionStore,
                        workflowConfig,
                        MoreExecutors.listeningDecorator(readExecutor),
                        MoreExecutors.listeningDecorator(writeExecutor)),
                new DurableWritesInvariantMetricReporter(
                        SingleBusyCellReadNoTouchWorkflows.class.getSimpleName(),
                        DurableWritesMetrics.of(taggedMetricRegistry)),
                SerializableInvariantLogReporter.INSTANCE);
    }

    private WorkflowAndInvariants<Workflow> createRingWorkflowValidator(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            RingWorkflowConfiguration workflowConfig,
            LifecycleEnvironment lifecycle) {
        ExecutorService executorService = createExecutorService(workflowConfig, lifecycle, RingWorkflows.class);
        InteractiveTransactionStore transactionStore =
                transactionStoreFactory.create(workflowConfig.tableConfiguration());
        return WorkflowAndInvariants.of(RingWorkflows.create(
                transactionStore, workflowConfig, MoreExecutors.listeningDecorator(executorService)));
    }

    private WorkflowAndInvariants<Workflow> createSingleRowTwoCellsWorkflowValidator(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            SingleRowTwoCellsWorkflowConfiguration workflowConfig,
            LifecycleEnvironment lifecycle) {
        ExecutorService executorService =
                createExecutorService(workflowConfig, lifecycle, SingleRowTwoCellsWorkflows.class);
        return WorkflowAndInvariants.builder()
                .workflow(SingleRowTwoCellsWorkflows.createSingleRowTwoCell(
                        transactionStoreFactory.create(workflowConfig.tableConfiguration()),
                        workflowConfig,
                        MoreExecutors.listeningDecorator(executorService)))
                .addInvariantReporters(new DurableWritesInvariantMetricReporter(
                        SingleRowTwoCellsWorkflows.class.getSimpleName(),
                        DurableWritesMetrics.of(taggedMetricRegistry)))
                .addInvariantReporters(SerializableInvariantLogReporter.INSTANCE)
                .build();
    }

    private WorkflowAndInvariants<Workflow> createBankBalanceWorkflow(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            BankBalanceWorkflowConfiguration workflowConfig,
            LifecycleEnvironment lifecycle) {
        ExecutorService executorService = createExecutorService(workflowConfig, lifecycle, BankBalanceWorkflows.class);
        InteractiveTransactionStore transactionStore =
                transactionStoreFactory.create(workflowConfig.tableConfiguration());
        return WorkflowAndInvariants.builder()
                .workflow(BankBalanceWorkflows.create(
                        transactionStore, workflowConfig, MoreExecutors.listeningDecorator(executorService)))
                .addInvariantReporters(new DurableWritesInvariantMetricReporter(
                        BankBalanceWorkflows.class.getSimpleName(), DurableWritesMetrics.of(taggedMetricRegistry)))
                .build();
    }

    private WorkflowAndInvariants<Workflow> createRandomWorkflow(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            RandomWorkflowConfiguration workflowConfig,
            LifecycleEnvironment lifecycle) {
        ExecutorService executorService = createExecutorService(workflowConfig, lifecycle, RandomWorkflows.class);
        TransactionStore transactionStore = transactionStoreFactory.create(workflowConfig.tableConfiguration());
        return WorkflowAndInvariants.builder()
                .workflow(RandomWorkflows.create(
                        transactionStore, workflowConfig, MoreExecutors.listeningDecorator(executorService)))
                .addInvariantReporters(new DurableWritesInvariantMetricReporter(
                        RandomWorkflows.class.getSimpleName(), DurableWritesMetrics.of(taggedMetricRegistry)))
                .addInvariantReporters(SerializableInvariantLogReporter.INSTANCE)
                .build();
    }

    private WorkflowAndInvariants<Workflow> createWriteOnceDeleteOnceWorkflow(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            WriteOnceDeleteOnceWorkflowConfiguration workflowConfig,
            LifecycleEnvironment lifecycle) {
        ExecutorService executorService =
                createExecutorService(workflowConfig, lifecycle, WriteOnceDeleteOnceWorkflows.class);
        InteractiveTransactionStore transactionStore =
                transactionStoreFactory.create(workflowConfig.tableConfiguration());
        return WorkflowAndInvariants.builder()
                .workflow(WriteOnceDeleteOnceWorkflows.create(
                        transactionStore, workflowConfig, MoreExecutors.listeningDecorator(executorService)))
                .addInvariantReporters(new DurableWritesInvariantMetricReporter(
                        WriteOnceDeleteOnceWorkflows.class.getSimpleName(),
                        DurableWritesMetrics.of(taggedMetricRegistry)))
                .addInvariantReporters(SerializableInvariantLogReporter.INSTANCE)
                .build();
    }

    @VisibleForTesting
    CountDownLatch workflowsRanLatch() {
        return workflowsRanLatch;
    }

    @VisibleForTesting
    TaggedMetricRegistry getTaggedMetricRegistry() {
        return taggedMetricRegistry;
    }

    private static <T> ExecutorService createExecutorService(
            WorkflowConfiguration workflowConfig, LifecycleEnvironment lifecycle, Class<T> workflowFactoryClass) {
        return lifecycle
                .executorService(workflowFactoryClass.getSimpleName())
                .minThreads(workflowConfig.maxThreadCount())
                .maxThreads(workflowConfig.maxThreadCount())
                .build();
    }
}
