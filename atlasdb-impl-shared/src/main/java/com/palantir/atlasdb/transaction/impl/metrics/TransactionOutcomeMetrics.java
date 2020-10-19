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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Tracks various states of an AtlasDB transaction, and marks associated meters in the
 * {@link com.palantir.tritium.metrics.registry.TaggedMetricRegistry} associated with the provided
 * {@link MetricsManager}. See {@link TransactionOutcome} for a more detailed explanation of the various
 * outcomes.
 */
public class TransactionOutcomeMetrics {
    @VisibleForTesting
    final MetricsManager metricsManager;

    private final Predicate<TableReference> safeForLogging;

    @VisibleForTesting
    TransactionOutcomeMetrics(MetricsManager metricsManager, Predicate<TableReference> safeForLogging) {
        this.metricsManager = metricsManager;
        this.safeForLogging = safeForLogging;
    }

    public static TransactionOutcomeMetrics create(MetricsManager metricsManager) {
        return new TransactionOutcomeMetrics(metricsManager, LoggingArgs::isSafe);
    }

    public void markSuccessfulCommit() {
        getMeter(TransactionOutcome.SUCCESSFUL_COMMIT).mark();
    }

    public void markFailedCommit() {
        getMeter(TransactionOutcome.FAILED_COMMIT).mark();
    }

    public void markAbort() {
        getMeter(TransactionOutcome.ABORT).mark();
    }

    public void markWriteWriteConflict(TableReference tableReference) {
        getMeterForTable(TransactionOutcome.WRITE_WRITE_CONFLICT, tableReference).mark();
    }

    public void markReadWriteConflict(TableReference tableReference) {
        getMeterForTable(TransactionOutcome.READ_WRITE_CONFLICT, tableReference).mark();
    }

    public void markLocksExpired() {
        getMeter(TransactionOutcome.LOCKS_EXPIRED).mark();
    }

    public void markPreCommitCheckFailed() {
        getMeter(TransactionOutcome.PRE_COMMIT_CHECK_FAILED).mark();
    }

    public void markPutUnlessExistsFailed() {
        getMeter(TransactionOutcome.PUT_UNLESS_EXISTS_FAILED).mark();
    }

    public void markRollbackOtherTransaction() {
        getMeter(TransactionOutcome.ROLLBACK_OTHER).mark();
    }

    @VisibleForTesting
    Meter getMeter(TransactionOutcome outcome) {
        return getMeter(outcome, ImmutableMap.of());
    }

    private Meter getMeter(TransactionOutcome outcome, Map<String, String> safeTags) {
        return metricsManager.getTaggedRegistry().meter(
                getMetricName(outcome, safeTags));
    }

    private Meter getMeterForTable(TransactionOutcome outcome, TableReference tableReference) {
        TableReference safeTableReference = safeForLogging.test(tableReference)
                ? tableReference
                : LoggingArgs.PLACEHOLDER_TABLE_REFERENCE;
        return getMeter(outcome, ImmutableMap.of("tableReference", safeTableReference.getQualifiedName()));
    }

    @VisibleForTesting
    MetricName getMetricName(TransactionOutcome outcome, Map<String, String> safeTags) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(TransactionOutcomeMetrics.class, outcome.name()))
                .putAllSafeTags(safeTags)
                .build();
    }
}
