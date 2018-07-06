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

import java.util.function.LongConsumer;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.WritableAssertionInfo;
import org.assertj.core.internal.Objects;

import com.codahale.metrics.Metric;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class TransactionOutcomeMetricsAssert extends
        AbstractAssert<TransactionOutcomeMetricsAssert, TransactionOutcomeMetrics> {
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Objects objects = Objects.instance();
    private WritableAssertionInfo writableAssertionInfo = new WritableAssertionInfo();

    public TransactionOutcomeMetricsAssert(TransactionOutcomeMetrics actual) {
        super(actual, TransactionOutcomeMetricsAssert.class);
        taggedMetricRegistry = actual.metricsManager.getTaggedRegistry();
    }

    public static TransactionOutcomeMetricsAssert assertThat(TransactionOutcomeMetrics actual) {
        return new TransactionOutcomeMetricsAssert(actual);
    }

    public TransactionOutcomeMetricsAssert hasSuccessfulCommits(long count) {
        checkPresentAndCheckCount("successfulCommit", count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasFailedCommits(long count) {
        checkPresentAndCheckCount("failedCommit", count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasAborts(long count) {
        checkPresentAndCheckCount("abort", count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasRollbackOther(long count) {
        checkPresentAndCheckCount("rollbackOther", count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasLocksExpired(long count) {
        checkPresentAndCheckCount("locksExpired", count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasPutUnlessExistsFailed(long count) {
        checkPresentAndCheckCount("putUnlessExistsFailed", count);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasNamedReadWriteConflicts(TableReference tableReference, long count) {
        checkPresentAndCheckCount("readWriteConflict", count, tableReference);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasNamedWriteWriteConflicts(TableReference tableReference, long count) {
        checkPresentAndCheckCount("writeWriteConflict", count, tableReference);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasPlaceholderReadWriteConflicts(long count) {
        checkPresentAndCheckCount("readWriteConflict", count, LoggingArgs.PLACEHOLDER_TABLE_REFERENCE);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasPlaceholderWriteWriteConflicts(long count) {
        checkPresentAndCheckCount("writeWriteConflict", count, LoggingArgs.PLACEHOLDER_TABLE_REFERENCE);
        return this;
    }

    public TransactionOutcomeMetricsAssert hasPlaceholderWriteWriteConflictsSatisfying(LongConsumer assertion) {
        MetricName metricName = actual.getMetricName("writeWriteConflict",
                getTableReferenceTags(LoggingArgs.PLACEHOLDER_TABLE_REFERENCE));
        assertion.accept(taggedMetricRegistry.meter(metricName).getCount());
        return this;
    }

    public TransactionOutcomeMetricsAssert hasNoKnowledgeOf(TableReference tableReference) {
        assertMetricNotExists(actual.getMetricName("readWriteConflict", getTableReferenceTags(tableReference)));
        assertMetricNotExists(actual.getMetricName("writeWriteConflict", getTableReferenceTags(tableReference)));
        return this;
    }

    private ImmutableMap<String, String> getTableReferenceTags(TableReference tableReference) {
        return ImmutableMap.of("tableReference", tableReference.getQualifiedName());
    }

    private void checkPresentAndCheckCount(String name, long count) {
        MetricName metricName = actual.getMetricName(name, ImmutableMap.of());
        checkPresentAndCheckCount(metricName, count);
    }

    private void checkPresentAndCheckCount(String name, long count, TableReference tableReference) {
        MetricName metricName = actual.getMetricName(name, getTableReferenceTags(tableReference));
        checkPresentAndCheckCount(metricName, count);
    }

    private void checkPresentAndCheckCount(MetricName metricName, long count) {
        assertMetricExists(metricName);
        objects.assertEqual(writableAssertionInfo, taggedMetricRegistry.meter(metricName).getCount(), count);
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
