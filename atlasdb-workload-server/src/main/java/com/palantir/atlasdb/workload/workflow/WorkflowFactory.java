/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.workload.DurableWritesMetrics;
import com.palantir.atlasdb.workload.config.WorkloadServerInstallConfiguration;
import com.palantir.atlasdb.workload.invariant.DurableWritesInvariantMetricReporter;
import com.palantir.atlasdb.workload.invariant.SerializableInvariantLogReporter;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStoreFactory;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.workflow.bank.BankBalanceWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.bank.BankBalanceWorkflows;
import com.palantir.atlasdb.workload.workflow.ring.RingWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.ring.RingWorkflows;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class WorkflowFactory {

    private static final SafeLogger log = SafeLoggerFactory.get(WorkflowFactory.class);
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final WorkflowExecutorFactory workflowExecutorFactory;

    public WorkflowFactory(TaggedMetricRegistry taggedMetricRegistry, WorkflowExecutorFactory workflowExecutorFactory) {
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.workflowExecutorFactory = workflowExecutorFactory;
    }

    public List<WorkflowAndInvariants<Workflow>> createAllWorkflowsAndInvariants(
            WorkloadServerInstallConfiguration installConfig, AtlasDbTransactionStoreFactory transactionStoreFactory) {
        SingleRowTwoCellsWorkflowConfiguration singleRowTwoCellsConfig = installConfig.singleRowTwoCellsConfig();
        RingWorkflowConfiguration ringWorkflowConfiguration = installConfig.ringConfig();
        TransientRowsWorkflowConfiguration transientRowsWorkflowConfiguration = installConfig.transientRowsConfig();
        SingleBusyCellWorkflowConfiguration singleBusyCellWorkflowConfiguration = installConfig.singleBusyCellConfig();
        SingleBusyCellReadNoTouchWorkflowConfiguration singleBusyCellReadNoTouchWorkflowConfiguration =
                installConfig.singleBusyCellReadsNoTouchConfig();
        BankBalanceWorkflowConfiguration bankBalanceConfig = installConfig.bankBalanceConfig();
        RandomWorkflowConfiguration randomWorkflowConfig = installConfig.randomConfig();
        WriteOnceDeleteOnceWorkflowConfiguration writeOnceDeleteOnceConfig = installConfig.writeOnceDeleteOnceConfig();
        MultipleBusyCellWorkflowConfiguration multipleBusyCellWorkflowConfig = installConfig.multipleBusyCellConfig();

        waitForTransactionStoreFactoryToBeInitialized(transactionStoreFactory);
        transactionStoreFactory.fastForwardTimestampToSupportTransactions3();

        return new ArrayList<>(List.of(
                createSingleRowTwoCellsWorkflowValidator(transactionStoreFactory, singleRowTwoCellsConfig),
                createRingWorkflowValidator(transactionStoreFactory, ringWorkflowConfiguration),
                createTransientRowsWorkflowValidator(transactionStoreFactory, transientRowsWorkflowConfiguration),
                createSingleBusyCellWorkflowValidator(transactionStoreFactory, singleBusyCellWorkflowConfiguration),
                createSingleBusyCellReadNoTouchWorkflowValidator(
                        transactionStoreFactory, singleBusyCellReadNoTouchWorkflowConfiguration),
                createBankBalanceWorkflow(transactionStoreFactory, bankBalanceConfig),
                createRandomWorkflow(transactionStoreFactory, randomWorkflowConfig),
                createWriteOnceDeleteOnceWorkflow(transactionStoreFactory, writeOnceDeleteOnceConfig),
                createMultipleBusyCellsWorkflow(transactionStoreFactory, multipleBusyCellWorkflowConfig)));
    }

    private static void waitForTransactionStoreFactoryToBeInitialized(AtlasDbTransactionStoreFactory factory) {
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
            AtlasDbTransactionStoreFactory transactionStoreFactory, TransientRowsWorkflowConfiguration workflowConfig) {
        ExecutorService executorService =
                workflowExecutorFactory.create(workflowConfig.maxThreadCount(), TransientRowsWorkflows.class);
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
            SingleBusyCellWorkflowConfiguration workflowConfig) {
        ExecutorService readExecutor = workflowExecutorFactory.create(
                workflowConfig.maxThreadCount() / 2, SingleBusyCellWorkflowConfiguration.class, "-read");
        ExecutorService writeExecutor = workflowExecutorFactory.create(
                workflowConfig.maxThreadCount() / 2, SingleBusyCellWorkflowConfiguration.class, "-write");
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
            SingleBusyCellReadNoTouchWorkflowConfiguration workflowConfig) {
        ExecutorService readExecutor = workflowExecutorFactory.create(
                workflowConfig.maxThreadCount() / 2, SingleBusyCellReadNoTouchWorkflowConfiguration.class, "-read");

        ExecutorService writeExecutor = workflowExecutorFactory.create(
                workflowConfig.maxThreadCount() / 2, SingleBusyCellReadNoTouchWorkflowConfiguration.class, "-write");

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
            AtlasDbTransactionStoreFactory transactionStoreFactory, RingWorkflowConfiguration workflowConfig) {
        ExecutorService executorService =
                workflowExecutorFactory.create(workflowConfig.maxThreadCount(), RingWorkflows.class);
        InteractiveTransactionStore transactionStore =
                transactionStoreFactory.create(workflowConfig.tableConfiguration());
        return WorkflowAndInvariants.of(RingWorkflows.create(
                transactionStore, workflowConfig, MoreExecutors.listeningDecorator(executorService)));
    }

    private WorkflowAndInvariants<Workflow> createSingleRowTwoCellsWorkflowValidator(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            SingleRowTwoCellsWorkflowConfiguration workflowConfig) {
        ExecutorService executorService =
                workflowExecutorFactory.create(workflowConfig.maxThreadCount(), SingleRowTwoCellsWorkflows.class);
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
            AtlasDbTransactionStoreFactory transactionStoreFactory, BankBalanceWorkflowConfiguration workflowConfig) {
        ExecutorService executorService =
                workflowExecutorFactory.create(workflowConfig.maxThreadCount(), BankBalanceWorkflows.class);
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
            AtlasDbTransactionStoreFactory transactionStoreFactory, RandomWorkflowConfiguration workflowConfig) {
        ExecutorService executorService =
                workflowExecutorFactory.create(workflowConfig.maxThreadCount(), RandomWorkflows.class);
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
            WriteOnceDeleteOnceWorkflowConfiguration workflowConfig) {
        ExecutorService executorService =
                workflowExecutorFactory.create(workflowConfig.maxThreadCount(), WriteOnceDeleteOnceWorkflows.class);
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

    private WorkflowAndInvariants<Workflow> createMultipleBusyCellsWorkflow(
            AtlasDbTransactionStoreFactory transactionStoreFactory,
            MultipleBusyCellWorkflowConfiguration workflowConfig) {
        ExecutorService readExecutorService = workflowExecutorFactory.create(
                workflowConfig.maxThreadCount() / 2, MultipleBusyCellWorkflows.class, "-read");
        ExecutorService writeExecutorService = workflowExecutorFactory.create(
                workflowConfig.maxThreadCount() / 2, MultipleBusyCellWorkflows.class, "-write");
        return WorkflowAndInvariants.builder()
                .workflow(MultipleBusyCellWorkflows.create(
                        transactionStoreFactory.create(workflowConfig.tableConfiguration()),
                        workflowConfig,
                        MoreExecutors.listeningDecorator(readExecutorService),
                        MoreExecutors.listeningDecorator(writeExecutorService)))
                .addInvariantReporters(new DurableWritesInvariantMetricReporter(
                        MultipleBusyCellWorkflows.class.getSimpleName(), DurableWritesMetrics.of(taggedMetricRegistry)))
                .addInvariantReporters(SerializableInvariantLogReporter.INSTANCE)
                .build();
    }
}
