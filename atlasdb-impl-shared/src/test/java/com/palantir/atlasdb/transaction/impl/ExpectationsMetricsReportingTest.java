/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Histogram;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionCommitLockInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.atlasdb.transaction.expectations.ExpectationsMetrics;
import com.palantir.atlasdb.util.MetricsManagers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class ExpectationsMetricsReportingTest {
    // immutable in type declaration for better 'withX' ergonomics
    private static final ImmutableTransactionReadInfo BLANK_READ_INFO =
            ImmutableTransactionReadInfo.builder().bytesRead(0).kvsCalls(0).build();

    private static final ImmutableTransactionCommitLockInfo BLANK_COMMIT_LOCK_INFO =
            ImmutableTransactionCommitLockInfo.builder()
                    .cellCommitLocksRequested(0L)
                    .rowCommitLocksRequested(0L)
                    .build();

    private ExpectationsMetrics metrics;

    @Before
    public void setUp() {
        metrics = ExpectationsMetrics.of(MetricsManagers.createForTests().getTaggedRegistry());
    }

    @Test
    public void ageReported() {
        long age = 129L;
        SnapshotTransaction.reportExpectationsCollectedData(age, BLANK_READ_INFO, BLANK_COMMIT_LOCK_INFO, metrics);
        assertHistogramHasSingleValue(metrics.ageMillis(), age);
    }

    @Test
    public void bytesReadReported() {
        long bytes = 1290L;
        reportMetricsWithReadInfo(BLANK_READ_INFO.withBytesRead(bytes));
        assertHistogramHasSingleValue(metrics.bytesRead(), bytes);
    }

    @Test
    public void kvsCallsReported() {
        long calls = 12L;
        reportMetricsWithReadInfo(BLANK_READ_INFO.withKvsCalls(calls));
        assertHistogramHasSingleValue(metrics.kvsReads(), calls);
    }

    @Test
    public void mostKvsBytesReadInSingleCallNotReportedOnReadInfoWithEmptyMaximumBytesKvsCallOnFinishedTransaction() {
        reportMetricsWithReadInfo(BLANK_READ_INFO);
        assertThat(metrics.mostKvsBytesReadInSingleCall().getSnapshot().getValues())
                .isEmpty();
    }

    @Test
    public void mostKvsBytesReadInSingleCallReportedOnReadInfoWithPresentMaximumBytesKvsCallOnFinishedTransaction() {
        long bytes = 193L;
        reportMetricsWithReadInfo(
                BLANK_READ_INFO.withMaximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.of("dummy", bytes)));
        assertHistogramHasSingleValue(metrics.mostKvsBytesReadInSingleCall(), bytes);
    }

    @Test
    public void commitLocksRequestsReported() {
        SnapshotTransaction.reportExpectationsCollectedData(
                0L,
                BLANK_READ_INFO,
                ImmutableTransactionCommitLockInfo.builder()
                        .cellCommitLocksRequested(1)
                        .rowCommitLocksRequested(2)
                        .build(),
                metrics);
        assertHistogramHasSingleValue(metrics.cellCommitLocksRequested(), 1);
        assertHistogramHasSingleValue(metrics.rowCommitLocksRequested(), 2);
    }

    private void reportMetricsWithReadInfo(TransactionReadInfo readInfo) {
        SnapshotTransaction.reportExpectationsCollectedData(0L, readInfo, BLANK_COMMIT_LOCK_INFO, metrics);
    }

    private void assertHistogramHasSingleValue(Histogram histogram, long expected) {
        assertThat(histogram.getSnapshot().getValues()).containsOnly(expected);
    }
}
