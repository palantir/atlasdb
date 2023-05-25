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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.KvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.atlasdb.transaction.expectations.ExpectationsMetrics;
import com.palantir.atlasdb.transaction.impl.ExpectationsAwareTransaction;
import com.palantir.atlasdb.transaction.impl.SnapshotTransaction;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public final class ExpectationsMetricsReportingTest {
    // immutable in type declaration for better 'withX' ergonomics
    private static final ImmutableTransactionReadInfo BLANK_READ_INFO =
            ImmutableTransactionReadInfo.builder().bytesRead(0).kvsCalls(0).build();

    @Mock
    ExpectationsAwareTransaction transaction;

    ExpectationsMetrics metrics =
            ExpectationsMetrics.of(MetricsManagers.createForTests().getTaggedRegistry());

    @Before
    public void setUp() {
        // avoids stub/mock NPEs
        when(transaction.getReadInfo()).thenReturn(BLANK_READ_INFO);
    }

    @Test
    public void noMetricsAreReportedOnRunningTransaction() {
        ExpectationsMetrics spiedOnMetrics = spy(metrics);
        SnapshotTransaction.reportExpectationsCollectedData(transaction, spiedOnMetrics, true);
        verifyNoInteractions(spiedOnMetrics);
    }

    @Test
    public void ageReportedOnFinishedTransaction() {
        long age = 129L;
        when(transaction.getAgeMillis()).thenReturn(age);
        reportMetricsOnFinishedTransaction();
        assertThat(metrics.ageMillis().getSnapshot().getValues()).containsOnly(age);
    }

    @Test
    public void bytesReadReportedOnFinishedTransaction() {
        long bytes = 1290L;
        when(transaction.getReadInfo()).thenReturn(BLANK_READ_INFO.withBytesRead(bytes));
        reportMetricsOnFinishedTransaction();
        assertThat(metrics.bytesRead().getSnapshot().getValues()).containsOnly(bytes);
    }

    @Test
    public void kvsCallsReportedOnFinishedTransaction() {
        long calls = 12L;
        when(transaction.getReadInfo()).thenReturn(BLANK_READ_INFO.withKvsCalls(calls));
        reportMetricsOnFinishedTransaction();
        assertThat(metrics.kvsCalls().getSnapshot().getValues()).containsOnly(calls);
    }

    @Test
    public void worstKvsBytesReadNotReportedOnReadInfoWithEmptyMaximumBytesKvsCallOnFinishedTransaction() {
        reportMetricsOnFinishedTransaction();
        assertThat(metrics.worstKvsBytesRead().getSnapshot().getValues()).isEmpty();
    }

    @Test
    public void worstKvsBytesReadReportedOnReadInfoWithPresentMaximumBytesKvsCallOnFinishedTransaction() {
        long bytes = 193L;
        when(transaction.getReadInfo())
                .thenReturn(BLANK_READ_INFO.withMaximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.of("dummy", bytes)));
        reportMetricsOnFinishedTransaction();
        assertThat(metrics.worstKvsBytesRead().getSnapshot().getValues()).containsOnly(bytes);
    }

    private void reportMetricsOnFinishedTransaction() {
        SnapshotTransaction.reportExpectationsCollectedData(transaction, metrics, false);
    }
}
