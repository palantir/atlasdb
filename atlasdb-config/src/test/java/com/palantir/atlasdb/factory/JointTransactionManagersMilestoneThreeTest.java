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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.memory.ImmutableInstancedKeyValueServiceConfig;
import com.palantir.atlasdb.memory.InMemoryKeyValueServiceRegistry;
import com.palantir.atlasdb.table.description.GenericTestSchema;
import com.palantir.atlasdb.table.description.generated.GenericTestSchemaTableFactory;
import com.palantir.atlasdb.table.description.generated.RangeScanTestTable;
import com.palantir.atlasdb.table.description.generated.RangeScanTestTable.RangeScanTestRow;
import com.palantir.atlasdb.table.description.generated.SerializableRangeScanTestTable;
import com.palantir.atlasdb.table.description.generated.SerializableRangeScanTestTable.SerializableRangeScanTestRow;
import com.palantir.atlasdb.table.description.generated.SerializableRangeScanTestTable.SerializableRangeScanTestRowResult;
import com.palantir.atlasdb.transaction.api.ImmutableDependentState;
import com.palantir.atlasdb.transaction.api.ImmutableFullyCommittedState;
import com.palantir.atlasdb.transaction.api.ImmutablePrimaryTransactionLocator;
import com.palantir.atlasdb.transaction.api.ImmutableRolledBackState;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.DependentState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.FullyCommittedState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.RolledBackState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.Visitor;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.joint.JointTransactionManager;
import com.palantir.atlasdb.transaction.joint.SimpleJointTransactionManager;
import com.palantir.atlasdb.transaction.service.Transactions3Service;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

/**
 * These transaction managers operate with the assumption that their clocks have already been synchronized, i.e.
 * the identity timestamp translator is the correct behaviour.
 */
public class JointTransactionManagersMilestoneThreeTest {
    private static final RangeScanTestRow TEST_ROW = RangeScanTestRow.of("tom");
    private static final SerializableRangeScanTestRow SERIALIZABLE_TEST_ROW =
            SerializableRangeScanTestRow.of("serializable");
    private final InMemoryKeyValueServiceRegistry registry = new InMemoryKeyValueServiceRegistry();

    private TransactionManager txMgr1;
    private TransactionManager txMgr2;

    @Before
    public void setUp() {
        txMgr1 = TransactionManagers.builder()
                .config(ImmutableAtlasDbConfig.builder()
                        .keyValueService(ImmutableInstancedKeyValueServiceConfig.builder()
                                .namespace("one")
                                .registry(registry)
                                .concurrentGetRangesThreadPoolSize(1)
                                .build())
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

        txMgr2 = TransactionManagers.builder()
                .config(ImmutableAtlasDbConfig.builder()
                        .keyValueService(ImmutableInstancedKeyValueServiceConfig.builder()
                                .namespace("two")
                                .registry(registry)
                                .concurrentGetRangesThreadPoolSize(1)
                                .build())
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
    }

    @Test
    public void transitiveDependentLocatorsEndingInCommittedAreCommitted() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        AtomicLong timestamp = new AtomicLong();
        jtm.runTaskThrowOnConflict(managers -> {
            Transaction txn1 = managers.constituentTransactions().get("one");

            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);

            table1.putColumn1(TEST_ROW, 1L);
            timestamp.set(txn1.getTimestamp());
            return null;
        });

        Transactions3Service namespace1Service = Transactions3Service.create(registry.getKeyValueService("one"));
        TransactionCommittedState transactionCommittedState =
                namespace1Service.get(ImmutableList.of(timestamp.get())).get(timestamp.get());
        long commitTimestamp = transactionCommittedState.accept(new Visitor<>() {
            @Override
            public Long visitFullyCommitted(FullyCommittedState fullyCommittedState) {
                return fullyCommittedState.commitTimestamp();
            }

            @Override
            public Long visitRolledBack(RolledBackState rolledBackState) {
                throw new SafeIllegalStateException("Not expecting a rolled-back state!");
            }

            @Override
            public Long visitDependent(DependentState dependentState) {
                throw new SafeIllegalStateException("Not expecting a dependent state!");
            }
        });

        namespace1Service.checkAndSet(
                timestamp.get(),
                transactionCommittedState,
                ImmutableDependentState.builder()
                        .commitTimestamp(commitTimestamp)
                        .primaryLocator(ImmutablePrimaryTransactionLocator.builder()
                                .namespace("two")
                                .startTimestamp(9L)
                                .build())
                        .build());

        Transactions3Service namespace2Service = Transactions3Service.create(registry.getKeyValueService("two"));
        namespace2Service.putUnlessExists(
                9L,
                ImmutableDependentState.builder()
                        .commitTimestamp(commitTimestamp)
                        .primaryLocator(ImmutablePrimaryTransactionLocator.builder()
                                .namespace("three")
                                .startTimestamp(6L)
                                .build())
                        .build());
        Transactions3Service namespace3Service = Transactions3Service.create(registry.getKeyValueService("three"));
        namespace3Service.putUnlessExists(
                6L, ImmutableFullyCommittedState.builder().commitTimestamp(7L).build());

        Map<RangeScanTestRow, Long> ns1 = txMgr1.runTaskThrowOnConflict(txn -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn);
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).containsOnly(Maps.immutableEntry(TEST_ROW, 1L));
    }

    @Test
    public void transitiveDependentLocatorsEndingInRollbackAreRolledBack() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        AtomicLong timestamp = new AtomicLong();
        jtm.runTaskThrowOnConflict(managers -> {
            Transaction txn1 = managers.constituentTransactions().get("one");

            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);

            table1.putColumn1(TEST_ROW, 1L);
            timestamp.set(txn1.getTimestamp());
            return null;
        });

        Transactions3Service namespace1Service = Transactions3Service.create(registry.getKeyValueService("one"));
        TransactionCommittedState transactionCommittedState =
                namespace1Service.get(ImmutableList.of(timestamp.get())).get(timestamp.get());
        long commitTimestamp = transactionCommittedState.accept(new Visitor<>() {
            @Override
            public Long visitFullyCommitted(FullyCommittedState fullyCommittedState) {
                return fullyCommittedState.commitTimestamp();
            }

            @Override
            public Long visitRolledBack(RolledBackState rolledBackState) {
                throw new SafeIllegalStateException("Not expecting a rolled-back state!");
            }

            @Override
            public Long visitDependent(DependentState dependentState) {
                throw new SafeIllegalStateException("Not expecting a dependent state!");
            }
        });

        namespace1Service.checkAndSet(
                timestamp.get(),
                transactionCommittedState,
                ImmutableDependentState.builder()
                        .commitTimestamp(commitTimestamp)
                        .primaryLocator(ImmutablePrimaryTransactionLocator.builder()
                                .namespace("two")
                                .startTimestamp(9L)
                                .build())
                        .build());

        Transactions3Service namespace2Service = Transactions3Service.create(registry.getKeyValueService("two"));
        namespace2Service.putUnlessExists(
                9L,
                ImmutableDependentState.builder()
                        .commitTimestamp(commitTimestamp)
                        .primaryLocator(ImmutablePrimaryTransactionLocator.builder()
                                .namespace("three")
                                .startTimestamp(6L)
                                .build())
                        .build());
        Transactions3Service namespace3Service = Transactions3Service.create(registry.getKeyValueService("three"));
        namespace3Service.putUnlessExists(6L, ImmutableRolledBackState.builder().build());

        Map<RangeScanTestRow, Long> ns1 = txMgr1.runTaskThrowOnConflict(txn -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn);
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).isEmpty();
    }

    @Test
    public void jointTransactionWritesVisibleInBothNamespaces() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");
        jtm.runTaskThrowOnConflict(managers -> {
            Transaction txn1 = managers.constituentTransactions().get("one");
            Transaction txn2 = managers.constituentTransactions().get("two");

            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

            table1.putColumn1(TEST_ROW, 1L);
            table2.putColumn1(TEST_ROW, 2L);
            return null;
        });

        Map<RangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).containsOnly(Maps.immutableEntry(TEST_ROW, 1L));
        Map<RangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns2).containsOnly(Maps.immutableEntry(TEST_ROW, 2L));
    }

    @Test
    public void failureInLeadTransactionManagerLeadsToRollback() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");
        AtomicLong timestamp = new AtomicLong(0); // naughty
        assertThatThrownBy(() -> jtm.runTaskThrowOnConflict(managers -> {
                    Transaction txn1 = managers.constituentTransactions().get("one");
                    Transaction txn2 = managers.constituentTransactions().get("two");

                    timestamp.set(txn1.getTimestamp());

                    RangeScanTestTable table1 =
                            GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
                    RangeScanTestTable table2 =
                            GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

                    table1.putColumn1(TEST_ROW, 1L);
                    table2.putColumn1(TEST_ROW, 2L);

                    Transactions3Service transactions3Service =
                            Transactions3Service.create(registry.getKeyValueService("one"));
                    transactions3Service.putUnlessExists(
                            txn1.getTimestamp(),
                            ImmutableRolledBackState.builder().build());
                    return null;
                }))
                .isInstanceOf(RuntimeException.class); // TODO (jkong): this needs to be more general

        assertRowUnwrittenInBothNamespaces(jtm);

        Transactions3Service transactions3Service = Transactions3Service.create(registry.getKeyValueService("two"));
        assertThat(transactions3Service.getImmediateState(timestamp.get()))
                .hasValueSatisfying(state -> assertThat(state).isInstanceOf(DependentState.class));
    }

    @Test
    public void failureInSecondaryTransactionManagerLeadsToRollback() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");
        AtomicLong timestamp = new AtomicLong(0); // naughty
        assertThatThrownBy(() -> jtm.runTaskThrowOnConflict(managers -> {
                    Transaction txn1 = managers.constituentTransactions().get("one");
                    Transaction txn2 = managers.constituentTransactions().get("two");

                    timestamp.set(txn1.getTimestamp());

                    RangeScanTestTable table1 =
                            GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
                    RangeScanTestTable table2 =
                            GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

                    table1.putColumn1(TEST_ROW, 1L);
                    table2.putColumn1(TEST_ROW, 2L);

                    Transactions3Service transactions3Service =
                            Transactions3Service.create(registry.getKeyValueService("two"));
                    transactions3Service.putUnlessExists(
                            txn1.getTimestamp(),
                            ImmutableRolledBackState.builder().build());
                    return null;
                }))
                .isInstanceOf(RuntimeException.class); // TODO (jkong): this needs to be more general

        assertRowUnwrittenInBothNamespaces(jtm);

        Transactions3Service transactions3Service = Transactions3Service.create(registry.getKeyValueService("one"));
        assertThat(transactions3Service.getImmediateState(timestamp.get()))
                .hasValueSatisfying(state -> assertThat(state).isInstanceOf(RolledBackState.class));
    }

    // This guarantees start1 < start2 < finish2 < finish1
    @Test
    public void singularDominatingWriteInLeadSpaceIsAConflict() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        ExecutorService executorService = PTExecutors.newCachedThreadPool();
        CountDownLatch twoCanStart = new CountDownLatch(1);
        CountDownLatch oneCanFinish = new CountDownLatch(1);

        Future<Object> firstTransaction = executorService.submit(() -> jtm.runTaskThrowOnConflict(managers -> {
            twoCanStart.countDown();
            Transaction txn1 = managers.constituentTransactions().get("one");
            Transaction txn2 = managers.constituentTransactions().get("two");

            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

            table1.putColumn1(TEST_ROW, 1L);
            table2.putColumn1(TEST_ROW, 2L);
            oneCanFinish.await();
            return null;
        }));
        Future<Object> secondTransaction = executorService.submit(() -> {
            twoCanStart.await();
            jtm.runTaskThrowOnConflict(managers -> {
                Transaction txn1 = managers.constituentTransactions().get("one");

                RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);

                table1.putColumn1(TEST_ROW, 3L);
                return null;
            });
            oneCanFinish.countDown();
            return null;
        });
        assertThatThrownBy(() -> Futures.getUnchecked(firstTransaction)).isInstanceOf(RuntimeException.class);
        Futures.getUnchecked(secondTransaction);
        Map<RangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).containsOnly(Maps.immutableEntry(TEST_ROW, 3L));
        Map<RangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns2).isEmpty();
    }

    // This guarantees start1 < start2 < finish2 < finish1
    @Test
    public void singularDominatingWriteInLeadSpaceWithConventionalLeadTransactionManagerIsAConflict() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        ExecutorService executorService = PTExecutors.newCachedThreadPool();
        CountDownLatch twoCanStart = new CountDownLatch(1);
        CountDownLatch oneCanFinish = new CountDownLatch(1);

        Future<Object> firstTransaction = executorService.submit(() -> jtm.runTaskThrowOnConflict(managers -> {
            twoCanStart.countDown();
            Transaction txn1 = managers.constituentTransactions().get("one");
            Transaction txn2 = managers.constituentTransactions().get("two");

            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

            table1.putColumn1(TEST_ROW, 1L);
            table2.putColumn1(TEST_ROW, 2L);
            oneCanFinish.await();
            return null;
        }));
        Future<Object> secondTransaction = executorService.submit(() -> {
            twoCanStart.await();
            txMgr1.runTaskThrowOnConflict(txn -> {
                RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn);
                table1.putColumn1(TEST_ROW, 3L);
                return null;
            });
            oneCanFinish.countDown();
            return null;
        });
        assertThatThrownBy(() -> Futures.getUnchecked(firstTransaction)).isInstanceOf(RuntimeException.class);
        Futures.getUnchecked(secondTransaction);
        Map<RangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).containsOnly(Maps.immutableEntry(TEST_ROW, 3L));
        Map<RangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns2).isEmpty();
    }

    // This guarantees start1 < start2 < finish2 < finish1
    @Test
    public void singularDominatingWriteInSecondarySpaceIsAConflict() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        ExecutorService executorService = PTExecutors.newCachedThreadPool();
        CountDownLatch twoCanStart = new CountDownLatch(1);
        CountDownLatch oneCanFinish = new CountDownLatch(1);

        Future<Object> firstTransaction = executorService.submit(() -> jtm.runTaskThrowOnConflict(managers -> {
            twoCanStart.countDown();
            Transaction txn1 = managers.constituentTransactions().get("one");
            Transaction txn2 = managers.constituentTransactions().get("two");

            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

            table1.putColumn1(TEST_ROW, 1L);
            table2.putColumn1(TEST_ROW, 2L);
            oneCanFinish.await();
            return null;
        }));
        Future<Object> secondTransaction = executorService.submit(() -> {
            twoCanStart.await();
            jtm.runTaskThrowOnConflict(managers -> {
                Transaction txn2 = managers.constituentTransactions().get("two");

                RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

                table2.putColumn1(TEST_ROW, 4L);
                return null;
            });
            oneCanFinish.countDown();
            return null;
        });
        assertThatThrownBy(() -> Futures.getUnchecked(firstTransaction)).isInstanceOf(RuntimeException.class);
        Futures.getUnchecked(secondTransaction);
        Map<RangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).isEmpty();
        Map<RangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns2).containsOnly(Maps.immutableEntry(TEST_ROW, 4L));
    }

    // This guarantees start1 < start2 < finish2 < finish1
    @Test
    public void singularDominatingWriteInSecondarySpaceWithConventionalSecondaryTransactionManagerIsAConflict() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        ExecutorService executorService = PTExecutors.newCachedThreadPool();
        CountDownLatch twoCanStart = new CountDownLatch(1);
        CountDownLatch oneCanFinish = new CountDownLatch(1);

        Future<Object> firstTransaction = executorService.submit(() -> jtm.runTaskThrowOnConflict(managers -> {
            twoCanStart.countDown();
            Transaction txn1 = managers.constituentTransactions().get("one");
            Transaction txn2 = managers.constituentTransactions().get("two");

            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

            table1.putColumn1(TEST_ROW, 1L);
            table2.putColumn1(TEST_ROW, 2L);
            oneCanFinish.await();
            return null;
        }));
        Future<Object> secondTransaction = executorService.submit(() -> {
            twoCanStart.await();
            txMgr2.runTaskThrowOnConflict(txn -> {
                RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn);

                table2.putColumn1(TEST_ROW, 4L);
                return null;
            });
            oneCanFinish.countDown();
            return null;
        });
        assertThatThrownBy(() -> Futures.getUnchecked(firstTransaction)).isInstanceOf(RuntimeException.class);
        Futures.getUnchecked(secondTransaction);
        Map<RangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).isEmpty();
        Map<RangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns2).containsOnly(Maps.immutableEntry(TEST_ROW, 4L));
    }

    // The intention is start1 < start2 < finish1 < finish2
    @Test
    public void singularSpanningWriteInLeadSpaceIsAConflict() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        ExecutorService executorService = PTExecutors.newCachedThreadPool();
        CountDownLatch twoCanStart = new CountDownLatch(1);
        CountDownLatch twoCanFinish = new CountDownLatch(1);

        Future<Object> firstTransaction = executorService.submit(() -> {
            jtm.runTaskThrowOnConflict(managers -> {
                twoCanStart.countDown();
                Transaction txn1 = managers.constituentTransactions().get("one");

                RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);

                table1.putColumn1(TEST_ROW, 1L);
                return null;
            });
            twoCanFinish.countDown();
            return null;
        });
        Future<Object> secondTransaction = executorService.submit(() -> {
            twoCanStart.await();
            jtm.runTaskThrowOnConflict(managers -> {
                Transaction txn1 = managers.constituentTransactions().get("one");
                Transaction txn2 = managers.constituentTransactions().get("two");

                RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
                RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

                table1.putColumn1(TEST_ROW, 3L);
                table2.putColumn1(TEST_ROW, 2L);
                twoCanFinish.await();
                return null;
            });
            return null;
        });
        Futures.getUnchecked(firstTransaction);
        assertThatThrownBy(() -> Futures.getUnchecked(secondTransaction)).isInstanceOf(RuntimeException.class);
        Map<RangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).containsOnly(Maps.immutableEntry(TEST_ROW, 1L));
        Map<RangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns2).isEmpty();
    }

    // The intention is start1 < start2 < finish1 < finish2
    @Test
    public void singularSpanningWriteInSecondarySpaceIsAConflict() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        ExecutorService executorService = PTExecutors.newCachedThreadPool();
        CountDownLatch twoCanStart = new CountDownLatch(1);
        CountDownLatch twoCanFinish = new CountDownLatch(1);

        Future<Object> firstTransaction = executorService.submit(() -> {
            jtm.runTaskThrowOnConflict(managers -> {
                twoCanStart.countDown();
                Transaction txn2 = managers.constituentTransactions().get("two");

                RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

                table2.putColumn1(TEST_ROW, 2L);
                return null;
            });
            twoCanFinish.countDown();
            return null;
        });
        Future<Object> secondTransaction = executorService.submit(() -> {
            twoCanStart.await();
            jtm.runTaskThrowOnConflict(managers -> {
                Transaction txn1 = managers.constituentTransactions().get("one");
                Transaction txn2 = managers.constituentTransactions().get("two");

                RangeScanTestTable table1 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn1);
                RangeScanTestTable table2 = GenericTestSchemaTableFactory.of().getRangeScanTestTable(txn2);

                table1.putColumn1(TEST_ROW, 1L);
                table2.putColumn1(TEST_ROW, 4L);
                twoCanFinish.await();
                return null;
            });
            return null;
        });
        Futures.getUnchecked(firstTransaction);
        assertThatThrownBy(() -> Futures.getUnchecked(secondTransaction)).isInstanceOf(RuntimeException.class);
        Map<RangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).isEmpty();
        Map<RangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns2).containsOnly(Maps.immutableEntry(TEST_ROW, 2L));
    }

    @Test
    public void highlyConcurrentTransactionsDoNotContendTooMuch() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));

            table1.putColumn1(TEST_ROW, 100L);
            table2.putColumn1(TEST_ROW, 100L);
            return null;
        });

        ExecutorService executorService = PTExecutors.newFixedThreadPool(25);
        List<Future<Long>> sumFutures = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            int finalI = i;
            sumFutures.add(executorService.submit(() -> {
                Uninterruptibles.sleepUninterruptibly(
                        ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                return jtm.runTaskThrowOnConflict(managers -> {
                    RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                            .getRangeScanTestTable(
                                    managers.constituentTransactions().get("one"));
                    RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                            .getRangeScanTestTable(
                                    managers.constituentTransactions().get("two"));

                    long valueInTableOne = Iterables.getOnlyElement(
                            table1.getColumn1s(ImmutableSet.of(TEST_ROW)).values());
                    long valueInTableTwo = Iterables.getOnlyElement(
                            table2.getColumn1s(ImmutableSet.of(TEST_ROW)).values());

                    if (finalI % 2 == 0) {
                        table1.putColumn1(TEST_ROW, valueInTableOne + 1);
                        table2.putColumn1(TEST_ROW, valueInTableTwo - 1);
                    } else {
                        table1.putColumn1(TEST_ROW, valueInTableOne - 1);
                        table2.putColumn1(TEST_ROW, valueInTableTwo + 1);
                    }

                    return valueInTableOne + valueInTableTwo;
                });
            }));
        }

        List<Long> totals = sumFutures.stream()
                .flatMap(future -> {
                    try {
                        return Stream.of(future.get());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        // Some conflicts are expected, that's fine
                        return Stream.of();
                    }
                })
                .collect(Collectors.toList());
        assertThat(totals).as("at least some stuff committed").hasSizeGreaterThan(5);
        assertThat(totals).as("at least some stuff contended").hasSizeLessThan(500);
        assertThat(totals)
                .as("the database is kept consistent, no non-transactional writes were seen")
                .allMatch(l -> l == 200L);
    }

    @Test
    public void readWriteConflictInLeadSpaceIsAConflict() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        ExecutorService executorService = PTExecutors.newCachedThreadPool();
        CountDownLatch twoCanStart = new CountDownLatch(1);
        CountDownLatch oneCanFinish = new CountDownLatch(1);

        Future<List<SerializableRangeScanTestRowResult>> firstTransaction =
                executorService.submit(() -> jtm.runTaskThrowOnConflict(managers -> {
                    twoCanStart.countDown();
                    Transaction txn1 = managers.constituentTransactions().get("one");
                    Transaction txn2 = managers.constituentTransactions().get("two");

                    SerializableRangeScanTestTable table1 =
                            GenericTestSchemaTableFactory.of().getSerializableRangeScanTestTable(txn1);
                    SerializableRangeScanTestTable table2 =
                            GenericTestSchemaTableFactory.of().getSerializableRangeScanTestTable(txn2);

                    List<SerializableRangeScanTestRowResult> rowResults = new ArrayList<>();
                    table1.getRange(RangeRequest.all()).batchAccept(10, item -> {
                        rowResults.addAll(item);
                        return true;
                    });
                    table2.putColumn1(SERIALIZABLE_TEST_ROW, 4L);
                    oneCanFinish.await();
                    return rowResults;
                }));

        Future<Object> secondTransaction = executorService.submit(() -> {
            twoCanStart.await();
            jtm.runTaskThrowOnConflict(managers -> {
                Transaction txn1 = managers.constituentTransactions().get("one");
                SerializableRangeScanTestTable table1 =
                        GenericTestSchemaTableFactory.of().getSerializableRangeScanTestTable(txn1);

                table1.putColumn1(SERIALIZABLE_TEST_ROW, 3L);
                return null;
            });
            oneCanFinish.countDown();
            return null;
        });
        assertThatThrownBy(() -> Futures.getUnchecked(firstTransaction)).isInstanceOf(RuntimeException.class);
        Futures.getUnchecked(secondTransaction);
        Map<SerializableRangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            SerializableRangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getSerializableRangeScanTestTable(
                            managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(SERIALIZABLE_TEST_ROW));
        });
        assertThat(ns1).containsOnly(Maps.immutableEntry(SERIALIZABLE_TEST_ROW, 3L));
        Map<SerializableRangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            SerializableRangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getSerializableRangeScanTestTable(
                            managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(SERIALIZABLE_TEST_ROW));
        });
        assertThat(ns2).isEmpty();
    }

    @Test
    public void readWriteConflictInSecondarySpaceIsAConflict() {
        JointTransactionManager jtm =
                new SimpleJointTransactionManager(ImmutableMap.of("one", txMgr1, "two", txMgr2), "one");

        ExecutorService executorService = PTExecutors.newCachedThreadPool();
        CountDownLatch twoCanStart = new CountDownLatch(1);
        CountDownLatch oneCanFinish = new CountDownLatch(1);

        Future<List<SerializableRangeScanTestRowResult>> firstTransaction =
                executorService.submit(() -> jtm.runTaskThrowOnConflict(managers -> {
                    twoCanStart.countDown();
                    Transaction txn1 = managers.constituentTransactions().get("one");
                    Transaction txn2 = managers.constituentTransactions().get("two");

                    SerializableRangeScanTestTable table1 =
                            GenericTestSchemaTableFactory.of().getSerializableRangeScanTestTable(txn1);
                    SerializableRangeScanTestTable table2 =
                            GenericTestSchemaTableFactory.of().getSerializableRangeScanTestTable(txn2);

                    List<SerializableRangeScanTestRowResult> rowResults = new ArrayList<>();
                    table1.putColumn1(SERIALIZABLE_TEST_ROW, 4L);
                    table2.getRange(RangeRequest.all()).batchAccept(10, item -> {
                        rowResults.addAll(item);
                        return true;
                    });
                    oneCanFinish.await();
                    return rowResults;
                }));

        Future<Object> secondTransaction = executorService.submit(() -> {
            twoCanStart.await();
            jtm.runTaskThrowOnConflict(managers -> {
                Transaction txn2 = managers.constituentTransactions().get("two");
                SerializableRangeScanTestTable table2 =
                        GenericTestSchemaTableFactory.of().getSerializableRangeScanTestTable(txn2);

                table2.putColumn1(SERIALIZABLE_TEST_ROW, 3L);
                return null;
            });
            oneCanFinish.countDown();
            return null;
        });
        assertThatThrownBy(() -> Futures.getUnchecked(firstTransaction)).isInstanceOf(RuntimeException.class);
        Futures.getUnchecked(secondTransaction);
        Map<SerializableRangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            SerializableRangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getSerializableRangeScanTestTable(
                            managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(SERIALIZABLE_TEST_ROW));
        });
        assertThat(ns1).isEmpty();
        Map<SerializableRangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            SerializableRangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getSerializableRangeScanTestTable(
                            managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(SERIALIZABLE_TEST_ROW));
        });
        assertThat(ns2).containsOnly(Maps.immutableEntry(SERIALIZABLE_TEST_ROW, 3L));
    }

    private void assertRowUnwrittenInBothNamespaces(JointTransactionManager jtm) {
        Map<RangeScanTestRow, Long> ns1 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table1 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("one"));
            return table1.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns1).isEmpty();
        Map<RangeScanTestRow, Long> ns2 = jtm.runTaskThrowOnConflict(managers -> {
            RangeScanTestTable table2 = GenericTestSchemaTableFactory.of()
                    .getRangeScanTestTable(managers.constituentTransactions().get("two"));
            return table2.getColumn1s(ImmutableSet.of(TEST_ROW));
        });
        assertThat(ns2).isEmpty();
    }
}
