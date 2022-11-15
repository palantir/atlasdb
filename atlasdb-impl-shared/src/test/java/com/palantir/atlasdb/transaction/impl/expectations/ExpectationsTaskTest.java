/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsAwareTransaction;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionViolationFlags;
import com.palantir.atlasdb.transaction.expectations.ExpectationsAlertingMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class ExpectationsTaskTest {

    @Mock
    private ExpectationsAwareTransaction transaction;

    @Mock
    private GaugesForExpectationsAlertingMetrics metrics;

    @Test
    public void taskHandlesExceptions() {
        when(transaction.checkAndGetViolations()).thenThrow(RuntimeException.class);
        ExpectationsTask task = new ExpectationsTask(ImmutableSet.of(transaction), metrics);
        task.run();
    }

    @Test
    public void noRegisteredTransactionsUpdatesMetricsCorrectly() {
        ;
    }

    @Test
    public void taskUpdatesMetricsCorrectly() {
        when(transaction.checkAndGetViolations())
                .thenReturn(ImmutableTransactionViolationFlags.builder()
                        .ranForTooLong(true)
                        .readTooMuch(false)
                        .readTooMuchInOneKvsCall(true)
                        .queriedKvsTooMuch(false)
                        .build());

        ExpectationsAwareTransaction transaction2 = mock(ExpectationsAwareTransaction.class);
        when(transaction2.checkAndGetViolations())
                .thenReturn(ImmutableTransactionViolationFlags.builder()
                        .ranForTooLong(true)
                        .readTooMuch(true)
                        .readTooMuchInOneKvsCall(false)
                        .queriedKvsTooMuch(false)
                        .build());

        MetricsManager metricsManager = MetricsManagers.createForTests();
        GaugesForExpectationsAlertingMetrics metrics = new GaugesForExpectationsAlertingMetrics(metricsManager);

        ExpectationsTask task = new ExpectationsTask(ImmutableSet.of(transaction, transaction2), metrics);
        task.run();

        assertThat(metricsManager
                        .getTaggedRegistry()
                        .gauge(ExpectationsAlertingMetrics.ranForTooLongMetricName())
                        .orElseThrow()
                        .getValue())
                .isEqualTo(true);

        assertThat(metricsManager
                        .getTaggedRegistry()
                        .gauge(ExpectationsAlertingMetrics.readTooMuchMetricName())
                        .orElseThrow()
                        .getValue())
                .isEqualTo(true);

        assertThat(metricsManager
                        .getTaggedRegistry()
                        .gauge(ExpectationsAlertingMetrics.readTooMuchInOneKvsCallMetricName())
                        .orElseThrow()
                        .getValue())
                .isEqualTo(true);

        assertThat(metricsManager
                        .getTaggedRegistry()
                        .gauge(ExpectationsAlertingMetrics.queriedKvsTooMuchMetricName())
                        .orElseThrow()
                        .getValue())
                .isEqualTo(false);
    }
}
