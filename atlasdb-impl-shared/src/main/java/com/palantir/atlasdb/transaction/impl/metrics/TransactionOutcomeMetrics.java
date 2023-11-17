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
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionMetrics.OutcomeBuildStage;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionMetrics.Outcome_Category;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.function.Predicate;

/**
 * Tracks various states of an AtlasDB transaction, and marks associated meters in the
 * {@link com.palantir.tritium.metrics.registry.TaggedMetricRegistry} associated with the provided
 * {@link MetricsManager}. See {@link TransactionOutcome} for a more detailed explanation of the various
 * outcomes.
 */
public class TransactionOutcomeMetrics {
    private final Predicate<TableReference> safeForLogging;
    private final TransactionMetrics metrics;

    @VisibleForTesting
    final TaggedMetricRegistry registry;

    @VisibleForTesting
    TransactionOutcomeMetrics(TaggedMetricRegistry metricRegistry, Predicate<TableReference> safeForLogging) {
        this.safeForLogging = safeForLogging;
        this.metrics = TransactionMetrics.of(metricRegistry);
        this.registry = metricRegistry;
    }

    public static TransactionOutcomeMetrics create(TaggedMetricRegistry metricRegistry) {
        return new TransactionOutcomeMetrics(metricRegistry, LoggingArgs::isSafe);
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
        getMeter(TransactionOutcome.WRITE_WRITE_CONFLICT, tableReference).mark();
    }

    public void markReadWriteConflict(TableReference tableReference) {
        getMeter(TransactionOutcome.READ_WRITE_CONFLICT, tableReference).mark();
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

    public void markCommitLockAcquisitionFailed() {
        getMeter(TransactionOutcome.COMMIT_LOCK_ACQUISITION_FAILED).mark();
    }

    @VisibleForTesting
    Meter getMeter(TransactionOutcome outcome) {
        return getMeter(outcome, "");
    }

    @VisibleForTesting
    Meter getMeter(TransactionOutcome outcome, TableReference tableReference) {
        TableReference safeTableReference =
                safeForLogging.test(tableReference) ? tableReference : LoggingArgs.PLACEHOLDER_TABLE_REFERENCE;
        return getMeter(outcome, safeTableReference.getQualifiedName());
    }

    private Meter getMeter(TransactionOutcome outcome, String tableReference) {
        return getMetric(outcome, tableReference).build();
    }

    @VisibleForTesting
    OutcomeBuildStage getMetric(TransactionOutcome outcome, String tableReference) {
        return metrics.outcome()
                .category(mapToMetricOutcome(outcome))
                .outcome(outcome.name())
                .tableReference(tableReference);
    }

    static TransactionMetrics.Outcome_Category mapToMetricOutcome(TransactionOutcome outcome) {
        switch (outcome) {
            case SUCCESSFUL_COMMIT:
                return Outcome_Category.SUCCESS;
            case FAILED_COMMIT:
            case ABORT:
            case WRITE_WRITE_CONFLICT:
            case READ_WRITE_CONFLICT:
            case LOCKS_EXPIRED:
            case PRE_COMMIT_CHECK_FAILED:
            case PUT_UNLESS_EXISTS_FAILED:
            case COMMIT_LOCK_ACQUISITION_FAILED:
                return Outcome_Category.FAIL;
            default:
                // To avoid throwing exceptions unnecessarily, we map all other unknown outcomes to UNKNOWN.
                // An unknown outcome should be added here as soon as possible. This won't be necessary when we have
                // covered switches from Java 17
                return Outcome_Category.UNKNOWN;
        }
    }
}
