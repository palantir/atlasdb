/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl;

import static com.palantir.atlasdb.transaction.impl.TaggedMetricRegistryAssert.assertThat;

import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class TransactionOutcomeMetricsTest {
    private static final Namespace NAMESPACE = Namespace.DEFAULT_NAMESPACE;

    private static final TableReference SAFE_REFERENCE_1 = TableReference.create(NAMESPACE, "safe1");
    private static final TableReference SAFE_REFERENCE_2 = TableReference.create(NAMESPACE, "safe2");
    private static final Set<TableReference> SAFE_REFERENCES = ImmutableSet.of(SAFE_REFERENCE_1, SAFE_REFERENCE_2);

    private static final TableReference UNSAFE_REFERENCE_1 = TableReference.create(NAMESPACE, "PII");
    private static final TableReference UNSAFE_REFERENCE_2 = TableReference.create(NAMESPACE, "topSecret");

    private static final TableReference PLACEHOLDER = LoggingArgs.PLACEHOLDER_TABLE_REFERENCE;

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();

    private final MetricsManager metricsManager = new MetricsManager(
            metricRegistry,
            taggedMetricRegistry,
            SAFE_REFERENCES::contains);
    private final TransactionOutcomeMetrics transactionOutcomeMetrics = new TransactionOutcomeMetrics(metricsManager,
            SAFE_REFERENCES::contains);

    @Before
    public void setUp() {
        ;
    }

    @Test
    public void canMarkOneSuccessfulCommit() {
        transactionOutcomeMetrics.markSuccessfulCommit();
        assertThat(taggedMetricRegistry).hasMeterWithCount(getMetricName("successfulCommit"), 1);
    }

    @Test
    public void canMarkMultipleSuccessfulCommits() {
        transactionOutcomeMetrics.markSuccessfulCommit();
        transactionOutcomeMetrics.markSuccessfulCommit();
        transactionOutcomeMetrics.markSuccessfulCommit();
        transactionOutcomeMetrics.markSuccessfulCommit();
        assertThat(taggedMetricRegistry).hasMeterWithCount(getMetricName("successfulCommit"), 4);
    }

    @Test
    public void canMarkVariousOutcomes() {
        Map<Integer, Runnable> tasks = ImmutableMap.of(
                1, transactionOutcomeMetrics::markAbort,
                2, transactionOutcomeMetrics::markSuccessfulCommit,
                3, transactionOutcomeMetrics::markLocksExpired,
                4, transactionOutcomeMetrics::markPutUnlessExistsFailed,
                5, transactionOutcomeMetrics::markRollbackOtherTransaction);

        tasks.entrySet().forEach(entry -> IntStream.range(0, entry.getKey()).forEach(unused -> entry.getValue().run()));

        assertThat(taggedMetricRegistry)
                .hasMeterWithCount(getMetricName("abort"), 1)
                .hasMeterWithCount(getMetricName("successfulCommit"), 2)
                .hasMeterWithCount(getMetricName("locksExpired"), 3)
                .hasMeterWithCount(getMetricName("putUnlessExistsFailed"), 4)
                .hasMeterWithCount(getMetricName("rollbackOther"), 5);
    }

    @Test
    public void tableReferencesIncludedAsTagIfSafe() {
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_1);
        transactionOutcomeMetrics.markWriteWriteConflict(SAFE_REFERENCE_1);

        assertThat(taggedMetricRegistry)
                .hasMeterWithCount(getTableReferenceTaggedMetricName("readWriteConflict", SAFE_REFERENCE_1), 2)
                .hasMeterWithCount(getTableReferenceTaggedMetricName("writeWriteConflict", SAFE_REFERENCE_1), 1);
    }

    @Test
    public void conflictsInDifferentTablesAreSeparateMetrics() {
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_2);

        assertThat(taggedMetricRegistry)
                .hasMeterWithCount(getTableReferenceTaggedMetricName("readWriteConflict", SAFE_REFERENCE_1), 1)
                .hasMeterWithCount(getTableReferenceTaggedMetricName("readWriteConflict", SAFE_REFERENCE_2), 1);
    }

    @Test
    public void conflictsInUnsafeTablesAreNotIncludedAsTags() {
        transactionOutcomeMetrics.markWriteWriteConflict(UNSAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_2);

        assertThat(taggedMetricRegistry)
                .doesNotHaveMeter(getTableReferenceTaggedMetricName("writeWriteConflict", UNSAFE_REFERENCE_1))
                .doesNotHaveMeter(getTableReferenceTaggedMetricName("readWriteConflict", UNSAFE_REFERENCE_2));
    }

    @Test
    public void conflictsInUnsafeTablesAreTrackedWithPlaceholder() {
        transactionOutcomeMetrics.markWriteWriteConflict(UNSAFE_REFERENCE_1);

        assertThat(taggedMetricRegistry)
                .hasMeterWithCount(getTableReferenceTaggedMetricName("writeWriteConflict", PLACEHOLDER), 1);
    }

    @Test
    public void conflictsAcrossMultipleUnsafeTablesAreTrackedWithTheSamePlaceholder() {
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_2);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_2);

        assertThat(taggedMetricRegistry)
                .hasMeterWithCount(getTableReferenceTaggedMetricName("readWriteConflict", PLACEHOLDER), 4);
    }

    @Test
    public void correctlyDistinguishesConflictsInSafeAndUnsafeTables() {
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_2);
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_2);

        assertThat(taggedMetricRegistry)
                .hasMeterWithCount(getTableReferenceTaggedMetricName("readWriteConflict", PLACEHOLDER), 2)
                .hasMeterWithCount(getTableReferenceTaggedMetricName("readWriteConflict", SAFE_REFERENCE_1), 1)
                .hasMeterWithCount(getTableReferenceTaggedMetricName("readWriteConflict", SAFE_REFERENCE_2), 1);
    }

    private MetricName getMetricName(String name) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(TransactionOutcomeMetrics.class, name))
                .build();
    }

    private MetricName getTableReferenceTaggedMetricName(String name, TableReference tableReference) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(TransactionOutcomeMetrics.class, name))
                .putSafeTags("tableReference", tableReference.getQualifiedName())
                .build();
    }
}
