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
import com.codahale.metrics.Metric;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.function.LongConsumer;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.WritableAssertionInfo;

public class TransactionOutcomeMetricsAssert
        extends AbstractAssert<TransactionOutcomeMetricsAssert, TransactionOutcomeMetrics> {
    private final WritableAssertionInfo writableAssertionInfo = new WritableAssertionInfo();
    private final TaggedMetricRegistry taggedMetricRegistry;

    public TransactionOutcomeMetricsAssert(TransactionOutcomeMetrics actual) {
        super(actual, TransactionOutcomeMetricsAssert.class);
        this.taggedMetricRegistry = actual.metricRegistry;
    }

    public static TransactionOutcomeMetricsAssert assertThat(TransactionOutcomeMetrics actual) {
        return new TransactionOutcomeMetricsAssert(actual);
    }

    public TransactionOutcomeMetricsAssert hasSuccessfulCommits(long count) {
        checkPresentAndCheckCount(TransactionOutcome.SUCCESSFUL_COMMIT, count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasFailedCommits(long count) {
        checkPresentAndCheckCount(TransactionOutcome.FAILED_COMMIT, count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasAborts(long count) {
        checkPresentAndCheckCount(TransactionOutcome.ABORT, count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasCommitLockAcquisitionFailures(long count) {
        checkPresentAndCheckCount(TransactionOutcome.COMMIT_LOCK_ACQUISITION_FAILED, count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasLocksExpired(long count) {
        checkPresentAndCheckCount(TransactionOutcome.LOCKS_EXPIRED, count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasPreCommitCheckFailures(long count) {
        checkPresentAndCheckCount(TransactionOutcome.PRE_COMMIT_CHECK_FAILED, count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasPutUnlessExistsFailures(long count) {
        checkPresentAndCheckCount(TransactionOutcome.PUT_UNLESS_EXISTS_FAILED, count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasNamedReadWriteConflicts(TableReference tableReference, long count) {
        checkPresentAndCheckCount(TransactionOutcome.READ_WRITE_CONFLICT, count, tableReference);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasNamedWriteWriteConflicts(TableReference tableReference, long count) {
        checkPresentAndCheckCount(TransactionOutcome.WRITE_WRITE_CONFLICT, count, tableReference);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasPlaceholderReadWriteConflicts(long count) {
        checkPresentAndCheckCount(
                TransactionOutcome.READ_WRITE_CONFLICT, count, LoggingArgs.PLACEHOLDER_TABLE_REFERENCE);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasPlaceholderWriteWriteConflicts(long count) {
        checkPresentAndCheckCount(
                TransactionOutcome.WRITE_WRITE_CONFLICT, count, LoggingArgs.PLACEHOLDER_TABLE_REFERENCE);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasPlaceholderWriteWriteConflictsSatisfying(LongConsumer assertion) {
        Meter meter = actual.getMeter(TransactionOutcome.WRITE_WRITE_CONFLICT, LoggingArgs.PLACEHOLDER_TABLE_REFERENCE);
        assertion.accept(meter.getCount());
        return this;
    }

    public TransactionOutcomeMetricsAssert hasNoKnowledgeOf(TableReference tableReference) {
        assertMetricNotExists(
                actual.getMetric(TransactionOutcome.READ_WRITE_CONFLICT, tableReference.getQualifiedName())
                        .buildMetricName());
        assertMetricNotExists(
                actual.getMetric(TransactionOutcome.WRITE_WRITE_CONFLICT, tableReference.getQualifiedName())
                        .buildMetricName());
        return this;
    }

    private void checkPresentAndCheckCount(TransactionOutcome outcome, long count) {
        MetricName metricName = actual.getMetric(outcome, "").buildMetricName();
        checkPresentAndCheckCount(metricName, count);
    }

    private void checkPresentAndCheckCount(TransactionOutcome outcome, long count, TableReference tableReference) {
        MetricName metricName =
                actual.getMetric(outcome, tableReference.getQualifiedName()).buildMetricName();
        checkPresentAndCheckCount(metricName, count);
    }

    private void checkPresentAndCheckCount(MetricName metricName, long count) {
        assertMetricExists(metricName);
        objects.assertEqual(
                writableAssertionInfo, taggedMetricRegistry.meter(metricName).getCount(), count);
    }

    private void assertMetricExists(MetricName metricName) {
        objects.assertNotNull(writableAssertionInfo, getMetric(metricName));
    }

    private void assertMetricNotExists(MetricName metricName) {
        objects.assertNull(writableAssertionInfo, getMetric(metricName));
    }

    private Metric getMetric(MetricName metricName) {
        return taggedMetricRegistry.getMetrics().get(metricName);
    }
}
