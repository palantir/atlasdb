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
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsData;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableExpectationsData;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionCommitLockInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionWriteMetadataInfo;
import com.palantir.atlasdb.transaction.expectations.ExpectationsMetrics;
import com.palantir.atlasdb.util.MetricsManagers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public final class ExpectationsMetricsReportingTest {

    // immutable in type declaration for better 'withX' ergonomics
    private static final ImmutableTransactionReadInfo BLANK_READ_INFO =
            ImmutableTransactionReadInfo.builder().bytesRead(0).kvsCalls(0).build();
    private static final ImmutableTransactionCommitLockInfo BLANK_COMMIT_LOCK_INFO =
            ImmutableTransactionCommitLockInfo.builder()
                    .cellCommitLocksRequested(0L)
                    .rowCommitLocksRequested(0L)
                    .build();
    private static final ImmutableTransactionWriteMetadataInfo BLANK_METADATA_INFO =
            ImmutableTransactionWriteMetadataInfo.builder()
                    .changeMetadataBuffered(0L)
                    .cellChangeMetadataSent(0L)
                    .rowChangeMetadataSent(0L)
                    .build();
    private static final ImmutableExpectationsData BLANK_EXPECTATIONS_DATA = ImmutableExpectationsData.builder()
            .ageMillis(0L)
            .readInfo(BLANK_READ_INFO)
            .commitLockInfo(BLANK_COMMIT_LOCK_INFO)
            .writeMetadataInfo(BLANK_METADATA_INFO)
            .build();

    private ExpectationsMetrics metrics;

    @BeforeEach
    public void setUp() {
        metrics = ExpectationsMetrics.of(MetricsManagers.createForTests().getTaggedRegistry());
    }

    @Test
    public void ageReported() {
        long age = 129L;
        reportMetrics(BLANK_EXPECTATIONS_DATA.withAgeMillis(age));
        assertHistogramHasSingleValue(metrics.ageMillis(), age);
    }

    @Test
    public void bytesReadReported() {
        long bytes = 1290L;
        reportMetrics(BLANK_EXPECTATIONS_DATA.withReadInfo(BLANK_READ_INFO.withBytesRead(bytes)));
        assertHistogramHasSingleValue(metrics.bytesRead(), bytes);
    }

    @Test
    public void kvsCallsReported() {
        long calls = 12L;
        reportMetrics(BLANK_EXPECTATIONS_DATA.withReadInfo(BLANK_READ_INFO.withKvsCalls(calls)));
        assertHistogramHasSingleValue(metrics.kvsReads(), calls);
    }

    @Test
    public void mostKvsBytesReadInSingleCallNotReportedOnReadInfoWithEmptyMaximumBytesKvsCallOnFinishedTransaction() {
        reportMetrics(BLANK_EXPECTATIONS_DATA);
        assertThat(metrics.mostKvsBytesReadInSingleCall().getSnapshot().getValues())
                .isEmpty();
    }

    @Test
    public void mostKvsBytesReadInSingleCallReportedOnReadInfoWithPresentMaximumBytesKvsCallOnFinishedTransaction() {
        long bytes = 193L;
        reportMetrics(BLANK_EXPECTATIONS_DATA.withReadInfo(
                BLANK_READ_INFO.withMaximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.of("dummy", bytes))));
        assertHistogramHasSingleValue(metrics.mostKvsBytesReadInSingleCall(), bytes);
    }

    @Test
    public void commitLocksRequestsReported() {
        reportMetrics(BLANK_EXPECTATIONS_DATA.withCommitLockInfo(ImmutableTransactionCommitLockInfo.builder()
                .cellCommitLocksRequested(1)
                .rowCommitLocksRequested(2)
                .build()));
        assertHistogramHasSingleValue(metrics.cellCommitLocksRequested(), 1);
        assertHistogramHasSingleValue(metrics.rowCommitLocksRequested(), 2);
    }

    @Test
    public void writeMetadataInfoReported() {
        reportMetrics(BLANK_EXPECTATIONS_DATA.withWriteMetadataInfo(ImmutableTransactionWriteMetadataInfo.builder()
                .changeMetadataBuffered(1)
                .cellChangeMetadataSent(2)
                .rowChangeMetadataSent(3)
                .build()));
        assertHistogramHasSingleValue(metrics.changeMetadataBuffered(), 1);
        assertHistogramHasSingleValue(metrics.cellChangeMetadataSent(), 2);
        assertHistogramHasSingleValue(metrics.rowChangeMetadataSent(), 3);
    }

    private void reportMetrics(ExpectationsData expectationsData) {
        SnapshotTransaction.reportExpectationsCollectedData(expectationsData, metrics);
    }

    private void assertHistogramHasSingleValue(Histogram histogram, long expected) {
        assertThat(histogram.getSnapshot().getValues()).containsOnly(expected);
    }
}
