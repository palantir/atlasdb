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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.errors.SerializableError;

public class ImmutableTimestampBridgingTimeLockServiceTest {
    private static final RemoteException REMOTE_EXCEPTION = new RemoteException(
            SerializableError.of("boo", RuntimeException.class), 404);
    private static final Exception WRAPPED_EXCEPTION = new AtlasDbRemoteException(REMOTE_EXCEPTION);
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

        whenStandardClientStartsTransactionNTimes(1);

        thenStartTransactionIsCalledNTimes(1);
        thenIndividualEndpointsAreCalledNTimes(1);
        thenNoCallsFailed();
    }

    @Test
    public void doesNotRepeatedlyTryAndFailToUseStartTransaction() {
        givenServerThrows404sOnStartTransaction();

        whenStandardClientStartsTransactionNTimes(10);

        // 2 because we are allowed to make a speculative try once per hour.
        thenStartTransactionIsCalledNTimes(2);
        thenIndividualEndpointsAreCalledNTimes(10);
        thenNoCallsFailed();
    }

    @Test
    public void performsStepsBatchedIfStartTransactionAvailable() {
        givenServerRespondsToStartTransaction();

        whenStandardClientStartsTransactionNTimes(1);

        thenStartTransactionIsCalledNTimes(1);
        thenNoCallsFailed();
    }

    @Test
    public void detectsStartTransactionBecomingAvailable() {
        givenServerThrows404sOnlyOnFirstThreeInvocations();

        whenQuickRetryingClientStartsTransactionNTimes(10);

        thenStartTransactionIsCalledNTimes(10);
        thenIndividualEndpointsAreCalledNTimes(3);
        thenNoCallsFailed();
    }

    @Test
    public void passesThroughOtherExceptions() {
        givenServerThrowsOnStartTransaction(RUNTIME_EXCEPTION);

        whenStandardClientStartsTransactionNTimes(5);

        thenStartTransactionIsCalledNTimes(5);
        thenCallsFailedWithExceptions(Collections.nCopies(5, RUNTIME_EXCEPTION));
    }

    private void givenServerThrows404sOnStartTransaction() {
        givenServerThrowsOnStartTransaction(WRAPPED_EXCEPTION);
    }

    private void givenServerRespondsToStartTransaction() {
        when(timelockService.startAtlasDbTransaction(any())).thenReturn(TRANSACTION_RESPONSE);
    }

    private void givenServerThrows404sOnlyOnFirstThreeInvocations() {
        when(timelockService.startAtlasDbTransaction(any()))
                .thenThrow(WRAPPED_EXCEPTION)
                .thenThrow(WRAPPED_EXCEPTION)
                .thenThrow(WRAPPED_EXCEPTION)
                .thenReturn(TRANSACTION_RESPONSE);
    }

    private void givenServerThrowsOnStartTransaction(Exception exception) {
        when(timelockService.startAtlasDbTransaction(any())).thenThrow(exception);
    }

    private void whenStandardClientStartsTransactionNTimes(int numCalls) {
        whenClientStartsTransactionNTimes(standardBridgingService, numCalls);
    }

    private void whenQuickRetryingClientStartsTransactionNTimes(int numCalls) {
        whenClientStartsTransactionNTimes(quickRetryingService, numCalls);
    }

    private void whenClientStartsTransactionNTimes(TimelockService client, int numCalls) {
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

    private void thenStartTransactionIsCalledNTimes(int numCalls) {
        verify(timelockService, times(numCalls)).startAtlasDbTransaction(IDENTIFIED_TIME_LOCK_REQUEST);
    }

    private void thenIndividualEndpointsAreCalledNTimes(int numCalls) {
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
