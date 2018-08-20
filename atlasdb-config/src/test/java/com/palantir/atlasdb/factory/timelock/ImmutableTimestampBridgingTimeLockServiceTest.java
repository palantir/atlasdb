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

package com.palantir.atlasdb.factory.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;

public class ImmutableTimestampBridgingTimeLockServiceTest {
    private static final AtlasDbRemoteException REMOTE_EXCEPTION_404 = mock(AtlasDbRemoteException.class);
    private static final AtlasDbRemoteException REMOTE_EXCEPTION_503 = mock(AtlasDbRemoteException.class);
    private static final Exception RUNTIME_EXCEPTION = new RuntimeException("boom");

    private static final long FRESH_TIMESTAMP = 1L;
    private static final LockImmutableTimestampResponse IMMUTABLE_TIMESTAMP_RESPONSE
            = LockImmutableTimestampResponse.of(0L, LockToken.of(UUID.randomUUID()));
    private static final StartAtlasDbTransactionResponse TRANSACTION_RESPONSE
            = StartAtlasDbTransactionResponse.of(IMMUTABLE_TIMESTAMP_RESPONSE, FRESH_TIMESTAMP);

    private static final IdentifiedTimeLockRequest IDENTIFIED_TIME_LOCK_REQUEST = IdentifiedTimeLockRequest.create();

    private List<Exception> caughtExceptions = Lists.newArrayList();

    private TimelockService timelockService = mock(TimelockService.class);
    private TimelockService quickRetryingService
            = new ImmutableTimestampBridgingTimeLockService(timelockService, RateLimiter.create(Double.MAX_VALUE));
    private TimelockService standardBridgingService
            = ImmutableTimestampBridgingTimeLockService.create(timelockService);

    @Before
    public void setUp() {
        when(REMOTE_EXCEPTION_404.getStatus()).thenReturn(404);
        when(REMOTE_EXCEPTION_503.getStatus()).thenReturn(503);

        when(timelockService.getFreshTimestamp()).thenReturn(1L);
        when(timelockService.lockImmutableTimestamp(any())).thenReturn(IMMUTABLE_TIMESTAMP_RESPONSE);
    }

    @After
    public void verifyMocks() {
        verifyNoMoreInteractions(timelockService);
    }

    @Test
    public void defersToIndividualStepsIfStartTransactionNotAvailable() {
        givenServerThrows404sOnStartTransaction();

        whenStandardClientStartsTransactionTimes(1);

        thenStartTransactionIsCalledTimes(1);
        thenIndividualEndpointsAreCalledTimes(1);
        thenNoCallsFailed();
    }

    @Test
    public void doesNotRepeatedlyTryAndFailToUseStartTransaction() {
        givenServerThrows404sOnStartTransaction();

        whenStandardClientStartsTransactionTimes(10);

        // 2 because we are allowed to make a speculative try once per hour.
        thenStartTransactionIsCalledTimes(2);
        thenIndividualEndpointsAreCalledTimes(10);
        thenNoCallsFailed();
    }

    @Test
    public void performsStepsBatchedIfStartTransactionAvailable() {
        givenServerRespondsToStartTransaction();

        whenStandardClientStartsTransactionTimes(1);

        thenStartTransactionIsCalledTimes(1);
        thenNoCallsFailed();
    }

    @Test
    public void detectsStartTransactionBecomingAvailable() {
        givenServerThrows404sOnlyOnFirstThreeInvocations();

        whenQuickRetryingClientStartsTransactionTimes(10);

        thenStartTransactionIsCalledTimes(10);
        thenIndividualEndpointsAreCalledTimes(3);
        thenNoCallsFailed();
    }

    @Test
    public void passesThroughExceptionsWithOtherErrorCodes() {
        givenServerThrowsOnStartTransaction(REMOTE_EXCEPTION_503);

        whenStandardClientStartsTransactionTimes(5);

        thenStartTransactionIsCalledTimes(5);
        thenCallsFailedWithExceptions(Collections.nCopies(5, REMOTE_EXCEPTION_503));
    }

    @Test
    public void passesThroughNonRemoteExceptions() {
        givenServerThrowsOnStartTransaction(RUNTIME_EXCEPTION);

        whenStandardClientStartsTransactionTimes(5);

        thenStartTransactionIsCalledTimes(5);
        thenCallsFailedWithExceptions(Collections.nCopies(5, RUNTIME_EXCEPTION));
    }

    @Test
    public void doesNotSwitchToSeparateEndpointsIfReceiving503() {
        givenServerThrowsAndThenSucceeds(REMOTE_EXCEPTION_503);

        whenStandardClientStartsTransactionTimes(5);

        thenStartTransactionIsCalledTimes(5);
        thenCallsFailedWithExceptions(ImmutableList.of(REMOTE_EXCEPTION_503));
    }

    @Test
    public void doesNotSwitchToSeparateEndpointsIfConnectionFails() {
        givenServerThrowsAndThenSucceeds(RUNTIME_EXCEPTION);

        whenStandardClientStartsTransactionTimes(5);

        thenStartTransactionIsCalledTimes(5);
        thenCallsFailedWithExceptions(ImmutableList.of(RUNTIME_EXCEPTION));
    }

    private void givenServerThrows404sOnStartTransaction() {
        givenServerThrowsOnStartTransaction(REMOTE_EXCEPTION_404);
    }

    private void givenServerRespondsToStartTransaction() {
        when(timelockService.startAtlasDbTransaction(any())).thenReturn(TRANSACTION_RESPONSE);
    }

    private void givenServerThrows404sOnlyOnFirstThreeInvocations() {
        when(timelockService.startAtlasDbTransaction(any()))
                .thenThrow(REMOTE_EXCEPTION_404)
                .thenThrow(REMOTE_EXCEPTION_404)
                .thenThrow(REMOTE_EXCEPTION_404)
                .thenReturn(TRANSACTION_RESPONSE);
    }

    private void givenServerThrowsAndThenSucceeds(Exception exception) {
        when(timelockService.startAtlasDbTransaction(any()))
                .thenThrow(exception)
                .thenReturn(TRANSACTION_RESPONSE);
    }

    private void givenServerThrowsOnStartTransaction(Exception exception) {
        when(timelockService.startAtlasDbTransaction(any())).thenThrow(exception);
    }

    private void whenStandardClientStartsTransactionTimes(int numCalls) {
        whenClientStartsTransactionTimes(standardBridgingService, numCalls);
    }

    private void whenQuickRetryingClientStartsTransactionTimes(int numCalls) {
        whenClientStartsTransactionTimes(quickRetryingService, numCalls);
    }

    private void whenClientStartsTransactionTimes(TimelockService client, int numCalls) {
        IntStream.range(0, numCalls)
                .forEach(unused -> {
                    try {
                        client.startAtlasDbTransaction(IDENTIFIED_TIME_LOCK_REQUEST);
                    } catch (Exception ex) {
                        // Expected in some tests
                        caughtExceptions.add(ex);
                    }
                });
    }

    private void thenStartTransactionIsCalledTimes(int numCalls) {
        verify(timelockService, times(numCalls)).startAtlasDbTransaction(IDENTIFIED_TIME_LOCK_REQUEST);
    }

    private void thenIndividualEndpointsAreCalledTimes(int numCalls) {
        verify(timelockService, times(numCalls)).lockImmutableTimestamp(IDENTIFIED_TIME_LOCK_REQUEST);
        verify(timelockService, times(numCalls)).getFreshTimestamp();
    }

    private void thenNoCallsFailed() {
        assertThat(caughtExceptions).isEmpty();
    }

    private void thenCallsFailedWithExceptions(List<Exception> exceptions) {
        assertThat(caughtExceptions).isEqualTo(exceptions);
    }
}
