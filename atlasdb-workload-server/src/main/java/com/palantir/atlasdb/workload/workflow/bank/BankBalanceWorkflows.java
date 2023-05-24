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
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.InteractiveTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.workflow.DefaultWorkflow;
import com.palantir.atlasdb.workload.workflow.Workflow;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import one.util.streamex.EntryStream;

public final class BankBalanceWorkflows {

    public static Workflow createSingleRowTwoCell(
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

    private static class BankBalanceRunTask {

        private static final Integer COLUMN = 0;
        private final AtomicBoolean skipRunning = new AtomicBoolean(false);

        private final BankBalanceWorkflowConfiguration workflowConfiguration;

        public BankBalanceRunTask(BankBalanceWorkflowConfiguration workflowConfiguration) {
            this.workflowConfiguration = workflowConfiguration;
        }

        public Optional<WitnessedTransaction> run(InteractiveTransactionStore store) {
            if (skipRunning.get()) {
                return Optional.empty();
            }
            workflowConfiguration.transactionRateLimiter().acquire();
            return store.readWrite(txn -> {
                if (isBalanceValid(txn)) {}
            });
        }

        private Map<Integer, Optional<Integer>> getAccountBalances(InteractiveTransaction transaction) {
            return IntStream.range(0, workflowConfiguration.numberOfAccounts())
                    .boxed()
                    .collect(Collectors.toMap(Function.identity(),
                            index -> transaction.read(workflowConfiguration.tableConfiguration().tableName(),
                                    getCellForAccount(index))));
        }

        private Map<Integer, Integer> generateBalances() {
            return IntStream.range(0, workflowConfiguration.numberOfAccounts())
                    .boxed()
                    .collect(Collectors.toMap(Function.identity(),
                            index -> workflowConfiguration.initialBalancePerAccount()));
        }

        private Optional<Map<Integer, Integer>> getOrInitializeBalances(InteractiveTransaction transaction) {
            Map<Integer, Optional<Integer>> balances = getAccountBalances(transaction);
            if (getAccountBalances(transaction).values().stream().allMatch(Optional::isPresent)) {
                return Optional.of(EntryStream.of(getAccountBalances(transaction)).mapValues(Optional::get).toMap());
            }

            if (balances.values().stream().anyMatch(Optional::isEmpty)) {
                skipRunning.set(true);
                return Optional.empty();
            }

            Map<Integer, Integer> balancesToWrite = generateBalances();



            return Optional.of(balancesToWrite);
        }

        private boolean isBalanceValid(InteractiveTransaction transaction) {
            Map<Integer, Integer> accountBalances =

            if (BankBalanceUtils.getMissingTotalBalance(balances.values(), totalBalance) != 0)) {
                log.error(
                        "Total balance is not correct. Expected: {}, Actual: {}",
                        workflowConfiguration.numAccounts() * workflowConfiguration.initialBalance(),
                        totalBalance);
                skipRunning.set(true);
                return false;
            }
            return true;
        }

        private WorkloadCell getCellForAccount(int accountIndex) {
            return ImmutableWorkloadCell.of(accountIndex, COLUMN);
        }
    }
}
