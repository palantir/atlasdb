/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.adjudicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceBlocking;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.client.ConjureTimelockServiceBlockingMetrics;
import com.palantir.lock.client.DialogueAdaptingConjureTimelockService;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class FeedbackMetricsTest {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer test");
    private static final String NAMESPACE = "test";
    private ConjureTimelockServiceBlockingMetrics metrics;
    private ConjureTimelockServiceBlocking conjureTimelockServiceBlocking = mock(ConjureTimelockServiceBlocking.class);
    private DialogueAdaptingConjureTimelockService service;

    @Mock
    private LeaderTime leaderTime;

    @Mock
    private ConjureStartTransactionsRequest request;

    @Mock
    private ConjureStartTransactionsResponse conjureStartTransactionsResponse;

    @Before
    public void cleanMetrics() {
        metrics = ConjureTimelockServiceBlockingMetrics.of(
                MetricsManagers.createForTests().getTaggedRegistry());
        service = new DialogueAdaptingConjureTimelockService(conjureTimelockServiceBlocking, metrics);
    }

    @Test
    public void leaderTimeMetricsAreRecordedOnSuccess() {
        when(conjureTimelockServiceBlocking.leaderTime(AUTH_HEADER, NAMESPACE)).thenReturn(leaderTime);
        service.leaderTime(AUTH_HEADER, NAMESPACE);
        assertThat(metrics.leaderTime().getCount()).isEqualTo(1L);
        assertThat(metrics.leaderTime().getSnapshot().get99thPercentile()).isNotZero();
        assertThat(metrics.leaderTimeErrors().getCount()).isEqualTo(0);
    }

    @Test
    public void leaderTimeErrorMetricsAreRecordedOnException() {
        when(conjureTimelockServiceBlocking.leaderTime(AUTH_HEADER, NAMESPACE))
                .thenThrow(new RuntimeException("Failed to get leader time."));
        assertThatThrownBy(() -> service.leaderTime(AUTH_HEADER, NAMESPACE))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to get leader time.");

        assertThat(metrics.leaderTime().getCount()).isEqualTo(1);
        assertThat(metrics.leaderTime().getSnapshot().get99thPercentile()).isNotZero();
        assertThat(metrics.leaderTimeErrors().getCount()).isEqualTo(1);
    }

    @Test
    public void startTransactionMetricsAreRecordedOnSuccess() {
        when(conjureTimelockServiceBlocking.startTransactions(AUTH_HEADER, NAMESPACE, request))
                .thenReturn(conjureStartTransactionsResponse);
        service.startTransactions(AUTH_HEADER, NAMESPACE, request);
        assertThat(metrics.startTransactions().getCount()).isEqualTo(1L);
        assertThat(metrics.startTransactions().getSnapshot().get99thPercentile())
                .isNotZero();
        assertThat(metrics.startTransactionErrors().getCount()).isEqualTo(0);
    }

    @Test
    public void startTransactionErrorMetricsAreRecordedOnException() {
        when(conjureTimelockServiceBlocking.startTransactions(AUTH_HEADER, NAMESPACE, request))
                .thenThrow(new RuntimeException("Failed to start transaction."));

        assertThatThrownBy(() -> service.startTransactions(AUTH_HEADER, NAMESPACE, request))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to start transaction.");

        assertThat(metrics.startTransactions().getCount()).isEqualTo(1);
        assertThat(metrics.startTransactions().getSnapshot().get99thPercentile())
                .isNotZero();
        assertThat(metrics.startTransactionErrors().getCount()).isEqualTo(1);
    }
}
