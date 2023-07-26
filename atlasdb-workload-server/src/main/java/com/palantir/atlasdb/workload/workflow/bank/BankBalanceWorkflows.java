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

package com.palantir.atlasdb.workload.workflow.bank;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.workload.store.ColumnAndValue;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.ColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.InteractiveTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.workflow.DefaultWorkflow;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * A workflow which performs a number of account balance transfers between accounts.
 */
public final class BankBalanceWorkflows {

    private static final SafeLogger log = SafeLoggerFactory.get(BankBalanceWorkflows.class);
    private static final SecureRandom RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();

    @VisibleForTesting
    static final Integer ROW = 42;

    private BankBalanceWorkflows() {
        // utility
    }

    public static Workflow create(
            InteractiveTransactionStore store,
            BankBalanceWorkflowConfiguration bankBalanceWorkflowConfiguration,
            ListeningExecutorService executionExecutor) {
        BankBalanceRunTask runTask = new BankBalanceRunTask(bankBalanceWorkflowConfiguration);
        return DefaultWorkflow.create(
                store,
                (txnStore, _index) -> runTask.run(txnStore),
                bankBalanceWorkflowConfiguration,
                executionExecutor);
    }

    @VisibleForTesting
    static Workflow create(
            InteractiveTransactionStore store,
            BankBalanceWorkflowConfiguration bankBalanceWorkflowConfiguration,
            ListeningExecutorService executionExecutor,
            AtomicBoolean skipRunning) {
        BankBalanceRunTask runTask = new BankBalanceRunTask(skipRunning, bankBalanceWorkflowConfiguration);
        return DefaultWorkflow.create(
                store,
                (txnStore, _index) -> runTask.run(txnStore),
                bankBalanceWorkflowConfiguration,
                executionExecutor);
    }

    private static class BankBalanceRunTask {
        private final AtomicBoolean skipRunning;

        private final BankBalanceWorkflowConfiguration workflowConfiguration;

        public BankBalanceRunTask(BankBalanceWorkflowConfiguration workflowConfiguration) {
            this(new AtomicBoolean(), workflowConfiguration);
        }

        @VisibleForTesting
        BankBalanceRunTask(AtomicBoolean skipRunning, BankBalanceWorkflowConfiguration workflowConfiguration) {
            this.skipRunning = skipRunning;
            this.workflowConfiguration = workflowConfiguration;
        }

        public Optional<WitnessedTransaction> run(InteractiveTransactionStore store) {
            if (skipRunning.get()) {
                return Optional.empty();
            }
            workflowConfiguration.transactionRateLimiter().acquire();
            return store.readWrite(txn -> {
                Map<Integer, Optional<Integer>> maybeBalances = getAccountBalances(txn);
                BankBalanceUtils.validateOrGenerateBalances(
                                maybeBalances,
                                workflowConfiguration.numberOfAccounts(),
                                workflowConfiguration.initialBalancePerAccount())
                        .ifPresentOrElse(balances -> performTransfers(txn, balances), () -> {
                            if (skipRunning.compareAndSet(false, true)) {
                                log.error(
                                        "Balance validation failed, indicating we have violated snapshot isolation.",
                                        SafeArg.of("maybeBalances", maybeBalances));
                            }
                        });
            });
        }

        private void performTransfers(InteractiveTransaction transaction, Map<Integer, Integer> balances) {
            Map<Integer, Integer> newBalances = BankBalanceUtils.performTransfers(
                    balances, RANDOM.nextInt(balances.size()), workflowConfiguration.transferAmount(), RANDOM);
            newBalances.forEach((account, balance) -> transaction.write(
                    workflowConfiguration.tableConfiguration().tableName(), getCellForAccount(account), balance));
        }

        private Map<Integer, Optional<Integer>> getAccountBalances(InteractiveTransaction transaction) {
            Map<Integer, Integer> balancesInDatabase = transaction
                    .getRowColumnRange(
                            workflowConfiguration.tableConfiguration().tableName(),
                            ROW,
                            ColumnRangeSelection.builder()
                                    .endColumnExclusive(workflowConfiguration.numberOfAccounts())
                                    .build())
                    .stream()
                    .collect(Collectors.toMap(ColumnAndValue::column, ColumnAndValue::value));
            return IntStream.range(0, workflowConfiguration.numberOfAccounts())
                    .boxed()
                    .collect(Collectors.toMap(
                            Function.identity(), index -> Optional.ofNullable(balancesInDatabase.get(index))));
        }
    }

    public static WorkloadCell getCellForAccount(int accountIndex) {
        return ImmutableWorkloadCell.of(ROW, accountIndex);
    }
}
