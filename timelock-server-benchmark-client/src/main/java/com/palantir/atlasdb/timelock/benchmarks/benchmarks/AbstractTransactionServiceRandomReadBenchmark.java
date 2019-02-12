/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionTables;

public abstract class AbstractTransactionServiceRandomReadBenchmark extends AbstractBenchmark {
    private final TransactionManager transactionManager;
    private final Map<Long, Long> timestampMap = Maps.newHashMap();
    private final Queue<Long> queryTimestampSource = new ConcurrentLinkedDeque<>();

    protected AbstractTransactionServiceRandomReadBenchmark(
            TransactionManager transactionManager,
            int numClients,
            int requestsPerClient) {
        super(numClients, requestsPerClient);

        this.transactionManager = transactionManager;
    }

    @Override
    protected void setup() {
        TransactionTables.truncateTables(transactionManager.getKeyValueService());
        populateTransactionTable();
    }

    private void populateTransactionTable() {
        timestampMap.putAll(getStartToCommitTimestampPairs());
        prepareExternalDependencies();
        transactionManager.getTransactionService().putUnlessExistsMultiple(timestampMap);
        List<Long> baseTimestamps = new ArrayList<>(timestampMap.keySet());
        Collections.shuffle(baseTimestamps);
        queryTimestampSource.addAll(baseTimestamps);
    }

    abstract Map<Long, Long> getStartToCommitTimestampPairs();

    abstract void prepareExternalDependencies();

    @Override
    protected void performOneCall() {
        Long timestampToQueryFor = queryTimestampSource.poll();
        Long commitTimestamp = transactionManager.getTransactionService().get(timestampToQueryFor);
        assertThat(commitTimestamp).isEqualTo(timestampMap.get(timestampToQueryFor));
    }

    @Override
    protected void cleanup() {
        TransactionTables.truncateTables(transactionManager.getKeyValueService());
    }
}
