/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.table.description.GenericTestSchema;
import com.palantir.atlasdb.table.description.generated.GenericTestSchemaTableFactory;
import com.palantir.atlasdb.table.description.generated.RangeScanTestTable;
import com.palantir.atlasdb.table.description.generated.RangeScanTestTable.RangeScanTestRow;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.joint.JointTransactionManager;
import com.palantir.atlasdb.transaction.joint.SimpleJointTransactionManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class JointTransactionManagersTest {
    @Test
    public void canDoJointTransactions() {
        TransactionManager txMgr1 = TransactionManagers.builder()
                .config(ImmutableAtlasDbConfig.builder()
                        .keyValueService(new InMemoryAtlasDbConfig())
                        .namespace("one")
                        .build())
                .userAgent(UserAgent.of(UserAgent.Agent.of("boo", "1.2.3")))
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar($ -> {})
                .addSchemas(GenericTestSchema.getSchema())
                .runtimeConfigSupplier(() -> Optional.of(ImmutableAtlasDbRuntimeConfig.builder()
                        .debugFlagInstallTransactionsThree(true)
                        .build()))
                .build()
                .serializable();

        TransactionManager txMgr2 = TransactionManagers.builder()
                .config(ImmutableAtlasDbConfig.builder()
                        .keyValueService(new InMemoryAtlasDbConfig())
                        .namespace("two")
                        .build())
                .userAgent(UserAgent.of(UserAgent.Agent.of("boo", "1.2.3")))
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar($ -> {})
                .addSchemas(GenericTestSchema.getSchema())
                .runtimeConfigSupplier(() -> Optional.of(ImmutableAtlasDbRuntimeConfig.builder()
                        .debugFlagInstallTransactionsThree(true)
                        .build()))
                .build()
                .serializable();

        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");
        jtm.runTaskThrowOnConflict(managers -> {
            Transaction txn1 = managers.constituentTransactions().get("one");
            Transaction txn2 = managers.constituentTransactions().get("two");

            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

            table1.putColumn1(RangeScanTestRow.of("tom"), 1L);
            table2.putColumn1(RangeScanTestRow.of("tom"), 2L);
            return null;
        });

        Map<RangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(RangeScanTestRow.of("tom")));
        });
        assertThat(ns1).containsOnly(Maps.immutableEntry(RangeScanTestRow.of("tom"), 1L));
        Map<RangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(RangeScanTestRow.of("tom")));
        });
        assertThat(ns2).containsOnly(Maps.immutableEntry(RangeScanTestRow.of("tom"), 2L));
    }
}
