/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class GapLimitingTransactionServiceTest {
    private static final long ONE = 1L;
    private static final long TWO = 2L;
    private static final long LARGE_TIMESTAMP = TransactionConstants.V2_PARTITIONING_QUANTUM * 1000L;

    private final TransactionService delegate = mock(TransactionService.class);
    private final TransactionService gapLimitingService = GapLimitingTransactionService.createDefaultForV2(delegate);

    @After
    public void verifyMocks() {
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void canGetFromDelegateEvenIfGapIsExceeded() {
        when(delegate.get(ONE)).thenReturn(LARGE_TIMESTAMP);
        assertThat(gapLimitingService.get(1L)).isEqualTo(TransactionConstants.V2_PARTITIONING_QUANTUM * 1000L);
        verify(delegate).get(eq(ONE));
    }

    @Test
    public void getMultipleCallsGetMultipleOnDelegate() {
        gapLimitingService.get(ImmutableList.of(ONE, TWO));
        verify(delegate).get(ImmutableList.of(ONE, TWO));
    }

    @Test
    public void defaultV2ServicePutsUnlessExistsWhereGapIsLessThanQuantum() {
        gapLimitingService.putUnlessExists(ONE, ONE + TWO);
        verify(delegate).putUnlessExists(ONE, ONE + TWO);
    }

    @Test
    public void defaultV2ServicePutsUnlessExistsWhereGapEqualsQuantum() {
        gapLimitingService.putUnlessExists(ONE, ONE + TransactionConstants.V2_PARTITIONING_QUANTUM);
        verify(delegate).putUnlessExists(ONE, ONE + TransactionConstants.V2_PARTITIONING_QUANTUM);
    }

    @Test
    public void defaultV2ServiceThrowsWhenAttemptingPutUnlessExistsWhereGapExceedsQuantum() {
        assertThatThrownBy(() -> gapLimitingService.putUnlessExists(ONE, LARGE_TIMESTAMP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Timestamp gap is too large");
    }

    @Test
    public void defaultV2ServiceStoresLargeTimestampsWithSmallLimits() {
        long veryLargeTimestamp = Long.MAX_VALUE - 1000;
        gapLimitingService.putUnlessExists(veryLargeTimestamp, veryLargeTimestamp + 8);
        verify(delegate).putUnlessExists(veryLargeTimestamp, veryLargeTimestamp + 8);
    }

    @Test
    public void defaultV2ServiceHandlesAbortsWhereStartTimestampExceedsQuantum() {
        gapLimitingService.putUnlessExists(LARGE_TIMESTAMP, TransactionConstants.FAILED_COMMIT_TS);
        verify(delegate).putUnlessExists(LARGE_TIMESTAMP, TransactionConstants.FAILED_COMMIT_TS);
    }

    @Test
    public void defaultV2ServiceDelegatesPutUnlessExistsMultipleAsOneCall() {
        Map<Long, Long> timestampData = ImmutableMap.of(ONE, TWO, 3L, 4L);
        gapLimitingService.putUnlessExistsMultiple(timestampData);
        verify(delegate).putUnlessExistsMultiple(timestampData);
    }

    @Test
    public void defaultV2ServiceThrowsOnPutUnlessExistsMultipleIfAnyPairsAreInvalid() {
        assertThatThrownBy(() -> gapLimitingService.putUnlessExistsMultiple(
                ImmutableMap.of(ONE, TWO, TWO, LARGE_TIMESTAMP)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Timestamp gap is too large");
    }

    @Test
    public void closeCallsCloseOnDelegate() {
        gapLimitingService.close();
        verify(delegate).close();
    }

    @Test
    public void throwsIfLimitSupplierReturnsNonPositiveValues() {
        TransactionService transactionService = new GapLimitingTransactionService(delegate, () -> 0L);
        assertThatThrownBy(() -> transactionService.putUnlessExists(ONE, TWO))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("limits less than 1 are unacceptable");

    }

    @Test
    public void throwsIfLimitSupplierReturnsNegativeValues() {
        TransactionService transactionService = new GapLimitingTransactionService(delegate, () -> -169L);
        assertThatThrownBy(() -> transactionService.putUnlessExists(ONE, TWO))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("limits less than 1 are unacceptable");

    }
}
