/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl.metrics;

import static com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetricsAssert.assertThat;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.Test;

public class TransactionOutcomeMetricsTest {
    private static final Namespace NAMESPACE = Namespace.DEFAULT_NAMESPACE;

    private static final TableReference SAFE_REFERENCE_1 = TableReference.create(NAMESPACE, "safe1");
    private static final TableReference SAFE_REFERENCE_2 = TableReference.create(NAMESPACE, "safe2");
    private static final Set<TableReference> SAFE_REFERENCES = ImmutableSet.of(SAFE_REFERENCE_1, SAFE_REFERENCE_2);

    private static final TableReference UNSAFE_REFERENCE_1 = TableReference.create(NAMESPACE, "PII");
    private static final TableReference UNSAFE_REFERENCE_2 = TableReference.create(NAMESPACE, "topSecret");

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();

    private final MetricsManager metricsManager = new MetricsManager(
            metricRegistry,
            taggedMetricRegistry,
            SAFE_REFERENCES::contains);
    private final TransactionOutcomeMetrics transactionOutcomeMetrics = new TransactionOutcomeMetrics(metricsManager,
            SAFE_REFERENCES::contains);

    @Test
    public void canMarkOneSuccessfulCommit() {
        transactionOutcomeMetrics.markSuccessfulCommit();
        assertThat(transactionOutcomeMetrics).hasSuccessfulCommits(1);
    }

    @Test
    public void canMarkMultipleSuccessfulCommits() {
        transactionOutcomeMetrics.markSuccessfulCommit();
        transactionOutcomeMetrics.markSuccessfulCommit();
        transactionOutcomeMetrics.markSuccessfulCommit();
        transactionOutcomeMetrics.markSuccessfulCommit();
        assertThat(transactionOutcomeMetrics).hasSuccessfulCommits(4);
    }

    @Test
    public void canMarkVariousOutcomes() {
        Map<Integer, Runnable> tasks = ImmutableMap.<Integer, Runnable>builderWithExpectedSize(6)
                .put(1, transactionOutcomeMetrics::markAbort)
                .put(2, transactionOutcomeMetrics::markSuccessfulCommit)
                .put(3, transactionOutcomeMetrics::markLocksExpired)
                .put(4, transactionOutcomeMetrics::markPutUnlessExistsFailed)
                .put(5, transactionOutcomeMetrics::markRollbackOtherTransaction)
                .put(6, transactionOutcomeMetrics::markPreCommitCheckFailed)
                .build();

        tasks.entrySet().forEach(entry -> IntStream.range(0, entry.getKey()).forEach(unused -> entry.getValue().run()));

        assertThat(transactionOutcomeMetrics)
                .hasAborts(1)
                .hasSuccessfulCommits(2)
                .hasLocksExpired(3)
                .hasPutUnlessExistsFailures(4)
                .hasRollbackOther(5)
                .hasPreCommitCheckFailures(6);
    }

    @Test
    public void tableReferencesIncludedAsTagIfSafe() {
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_1);
        transactionOutcomeMetrics.markWriteWriteConflict(SAFE_REFERENCE_1);

        assertThat(transactionOutcomeMetrics)
                .hasNamedReadWriteConflicts(SAFE_REFERENCE_1, 2)
                .hasNamedWriteWriteConflicts(SAFE_REFERENCE_1, 1);
    }

    @Test
    public void conflictsInDifferentTablesAreSeparateMetrics() {
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_2);

        assertThat(transactionOutcomeMetrics)
                .hasNamedReadWriteConflicts(SAFE_REFERENCE_1, 1)
                .hasNamedReadWriteConflicts(SAFE_REFERENCE_2, 1);
    }

    @Test
    public void conflictsInUnsafeTablesAreNotIncludedAsTags() {
        transactionOutcomeMetrics.markWriteWriteConflict(UNSAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_2);

        assertThat(transactionOutcomeMetrics)
                .hasNoKnowledgeOf(UNSAFE_REFERENCE_1)
                .hasNoKnowledgeOf(UNSAFE_REFERENCE_2);
    }

    @Test
    public void conflictsInUnsafeTablesAreTrackedWithPlaceholder() {
        transactionOutcomeMetrics.markWriteWriteConflict(UNSAFE_REFERENCE_1);

        assertThat(transactionOutcomeMetrics).hasPlaceholderWriteWriteConflicts(1);
    }

    @Test
    public void conflictsAcrossMultipleUnsafeTablesAreTrackedWithTheSamePlaceholder() {
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_2);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_2);

        assertThat(transactionOutcomeMetrics).hasPlaceholderReadWriteConflicts(4);
    }

    @Test
    public void correctlyDistinguishesConflictsInSafeAndUnsafeTables() {
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_1);
        transactionOutcomeMetrics.markReadWriteConflict(UNSAFE_REFERENCE_2);
        transactionOutcomeMetrics.markReadWriteConflict(SAFE_REFERENCE_2);

        assertThat(transactionOutcomeMetrics)
                .hasPlaceholderReadWriteConflicts(2)
                .hasNamedReadWriteConflicts(SAFE_REFERENCE_1, 1)
                .hasNamedReadWriteConflicts(SAFE_REFERENCE_2, 1);
    }
}
