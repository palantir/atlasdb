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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.expectations.ExpectationsMetrics;
import com.palantir.atlasdb.util.MetricsManagers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class ExpectationsMetricsReportingTest {
    // immutable in type declaration for better 'withX' ergonomics
    private static final ImmutableTransactionReadInfo BLANK_READ_INFO =
            ImmutableTransactionReadInfo.builder().bytesRead(0).kvsCalls(0).build();

    private static final ExpectationsMetrics METRICS =
            ExpectationsMetrics.of(MetricsManagers.createForTests().getTaggedRegistry());

    @Mock
    ExpectationsAwareTransaction transaction;

    @Before
    public void setUp() {
        // avoids stub/mock NPEs
        when(transaction.getReadInfo()).thenReturn(BLANK_READ_INFO);
    }

    @Test
    public void ageReported() {
        long age = 129L;
        when(transaction.getAgeMillis()).thenReturn(age);
        reportMetrics();
        assertThat(METRICS.ageMillis().getSnapshot().getValues()).containsOnly(age);
    }

    @Test
    public void bytesReadReported() {
        long bytes = 1290L;
        when(transaction.getReadInfo()).thenReturn(BLANK_READ_INFO.withBytesRead(bytes));
        reportMetrics();
        assertThat(METRICS.bytesRead().getSnapshot().getValues()).containsOnly(bytes);
    }

    @Test
    public void kvsCallsReported() {
        long calls = 12L;
        when(transaction.getReadInfo()).thenReturn(BLANK_READ_INFO.withKvsCalls(calls));
        reportMetrics();
        assertThat(METRICS.kvsReads().getSnapshot().getValues()).containsOnly(calls);
    }

    @Test
    public void mostKvsBytesReadInSingleCallNotReportedOnReadInfoWithEmptyMaximumBytesKvsCallOnFinishedTransaction() {
        reportMetrics();
        assertThat(METRICS.mostKvsBytesReadInSingleCall().getSnapshot().getValues())
                .isEmpty();
    }

    @Test
    public void mostKvsBytesReadInSingleCallReportedOnReadInfoWithPresentMaximumBytesKvsCallOnFinishedTransaction() {
        long bytes = 193L;
        when(transaction.getReadInfo())
                .thenReturn(BLANK_READ_INFO.withMaximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.of("dummy", bytes)));
        reportMetrics();
        assertThat(METRICS.mostKvsBytesReadInSingleCall().getSnapshot().getValues())
                .containsOnly(bytes);
    }

    private void reportMetrics() {
        SnapshotTransaction.reportExpectationsCollectedData(transaction, METRICS);
    }
}
