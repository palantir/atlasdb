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

import java.util.Map;
import java.util.function.Predicate;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.MetricName;

/**
 * Tracks various states of an AtlasDB transaction, and marks associated meters in the
 * {@link com.palantir.tritium.metrics.registry.TaggedMetricRegistry} associated with the provided
 * {@link MetricsManager}.
 */
public class TransactionOutcomeMetrics {
    private static final String SUCCESSFUL_COMMIT = "successfulCommit";
    private static final String FAILED_COMMIT = "failedCommit";
    private static final String ABORT = "abort";
    private static final String WRITE_WRITE_CONFLICT = "writeWriteConflict";
    private static final String READ_WRITE_CONFLICT = "readWriteConflict";
    private static final String LOCKS_EXPIRED = "locksExpired";
    private static final String PUT_UNLESS_EXISTS_FAILED = "putUnlessExistsFailed";
    private static final String ROLLBACK_OTHER = "rollbackOther";

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
        getMeter(SUCCESSFUL_COMMIT).mark();
    }

    public void markFailedCommit() {
        getMeter(FAILED_COMMIT).mark();
    }

    public void markAbort() {
        getMeter(ABORT).mark();
    }

    public void markWriteWriteConflict(TableReference tableReference) {
        getMeterForTable(WRITE_WRITE_CONFLICT, tableReference).mark();
    }

    public void markReadWriteConflict(TableReference tableReference) {
        getMeterForTable(READ_WRITE_CONFLICT, tableReference).mark();
    }

    public void markLocksExpired() {
        getMeter(LOCKS_EXPIRED).mark();
    }

    public void markPutUnlessExistsFailed() {
        getMeter(PUT_UNLESS_EXISTS_FAILED).mark();
    }

    public void markRollbackOtherTransaction() {
        getMeter(ROLLBACK_OTHER).mark();
    }

    @VisibleForTesting
    Meter getMeter(String meterName) {
        return getMeter(meterName, ImmutableMap.of());
    }

    private Meter getMeter(String meterName, Map<String, String> safeTags) {
        return metricsManager.getTaggedRegistry().meter(
                getMetricName(meterName, safeTags));
    }

    private Meter getMeterForTable(String meterName, TableReference tableReference) {
        TableReference safeTableReference = safeForLogging.test(tableReference)
                ? tableReference
                : LoggingArgs.PLACEHOLDER_TABLE_REFERENCE;
        return getMeter(meterName, ImmutableMap.of("tableReference", safeTableReference.getQualifiedName()));
    }

    @VisibleForTesting
    MetricName getMetricName(String meterName, Map<String, String> safeTags) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(TransactionOutcomeMetrics.class, meterName))
                .putAllSafeTags(safeTags)
                .build();
    }
}
