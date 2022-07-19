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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PreStartHandlingTransactionServiceTest {
    private final TransactionService delegate = mock(TransactionService.class);
    private final TransactionService preStartHandlingService = new PreStartHandlingTransactionService(delegate);

    private static final long START_TIMESTAMP = 44L;
    private static final long COMMIT_TIMESTAMP = 88L;
    private static final TransactionStatus COMMITTED = TransactionStatuses.committed(COMMIT_TIMESTAMP);
    private static final long UNCOMMITTED_START_TIMESTAMP = 999L;
    private static final long ZERO_TIMESTAMP = 0L;
    private static final long NEGATIVE_TIMESTAMP = -125L;
    private static final long BEFORE_TIME_TIMESTAMP = AtlasDbConstants.STARTING_TS - 1;

    private static final ImmutableList<Long> TWO_VALID_TIMESTAMPS =
            ImmutableList.of(START_TIMESTAMP, UNCOMMITTED_START_TIMESTAMP);
    private static final ImmutableList<Long> ONE_VALID_ONE_INVALID_TIMESTAMP =
            ImmutableList.of(START_TIMESTAMP, ZERO_TIMESTAMP);
    private static final ImmutableList<Long> TWO_INVALID_TIMESTAMPS =
            ImmutableList.of(ZERO_TIMESTAMP, NEGATIVE_TIMESTAMP);

    @Before
    public void setUpMocks() {
        when(delegate.get(START_TIMESTAMP)).thenReturn(COMMITTED);
        when(delegate.get(UNCOMMITTED_START_TIMESTAMP)).thenReturn(null);
        when(delegate.get(eq(TWO_VALID_TIMESTAMPS)))
                .thenReturn(ImmutableMap.of(
                        START_TIMESTAMP,
                        COMMITTED,
                        UNCOMMITTED_START_TIMESTAMP,
                        TransactionConstants.IN_PROGRESS_TRANSACTION));
        when(delegate.get(eq(ImmutableList.of(START_TIMESTAMP))))
                .thenReturn(ImmutableMap.of(START_TIMESTAMP, COMMITTED));
    }

    @After
    public void verifyMocks() {
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void passesThroughGetsOnValidCommittedTimestamp() {
        TransactionStatus status = preStartHandlingService.get(START_TIMESTAMP);
        assertThat(TransactionStatuses.getCommitTimestamp(status)).hasValue(COMMIT_TIMESTAMP);
        verify(delegate).get(START_TIMESTAMP);
    }

    @Test
    public void passesThroughGetsOnValidUncommittedTimestamp() {
        TransactionStatus status = preStartHandlingService.get(UNCOMMITTED_START_TIMESTAMP);
        assertThat(TransactionStatuses.caseOf(status).inProgress_(true).otherwise_(false))
                .isTrue();
        verify(delegate).get(UNCOMMITTED_START_TIMESTAMP);
    }

    @Test
    public void returnsTimestampBeforeStartingTimestampWhenGettingInvalidTimestamps() {
        assertThat(TransactionStatuses.getCommitTimestamp(preStartHandlingService.get(ZERO_TIMESTAMP)))
                .hasValue(BEFORE_TIME_TIMESTAMP);
        assertThat(TransactionStatuses.getCommitTimestamp(preStartHandlingService.get(NEGATIVE_TIMESTAMP)))
                .hasValue(BEFORE_TIME_TIMESTAMP);
    }

    @Test
    public void passesThroughGetsOnMultipleValidTimestamps() {
        Map<Long, TransactionStatus> result = preStartHandlingService.get(TWO_VALID_TIMESTAMPS);
        assertThat(result.size()).isEqualTo(2);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(START_TIMESTAMP)))
                .hasValue(COMMIT_TIMESTAMP);
        assertThat(TransactionStatuses.caseOf(result.get(UNCOMMITTED_START_TIMESTAMP))
                        .inProgress_(true)
                        .otherwise_(false))
                .isTrue();
        verify(delegate).get(eq(TWO_VALID_TIMESTAMPS));
    }

    @Test
    public void passesThroughOnlyValidTimestampsToDelegateWhenGettingMultiple() {
        Map<Long, TransactionStatus> result = preStartHandlingService.get(ONE_VALID_ONE_INVALID_TIMESTAMP);
        assertThat(result.size()).isEqualTo(2);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(START_TIMESTAMP)))
                .hasValue(COMMIT_TIMESTAMP);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(ZERO_TIMESTAMP)))
                .hasValue(BEFORE_TIME_TIMESTAMP);
        verify(delegate).get(eq(ImmutableList.of(START_TIMESTAMP)));
    }

    @Test
    public void doesNotInvokeDelegateIfNoValidTimestamps() {
        Map<Long, TransactionStatus> result = preStartHandlingService.get(TWO_INVALID_TIMESTAMPS);
        assertThat(result.size()).isEqualTo(2);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(ZERO_TIMESTAMP)))
                .hasValue(BEFORE_TIME_TIMESTAMP);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(NEGATIVE_TIMESTAMP)))
                .hasValue(BEFORE_TIME_TIMESTAMP);
        verify(delegate, never()).get(anyList());
    }

    @Test
    public void putUnlessExistsValidTimestampCallsDelegate() {
        preStartHandlingService.putUnlessExists(START_TIMESTAMP, COMMIT_TIMESTAMP);
        verify(delegate).putUnlessExists(START_TIMESTAMP, COMMIT_TIMESTAMP);
    }

    @Test
    public void propagatesPutUnlessExistsExceptions() {
        KeyAlreadyExistsException exception = new KeyAlreadyExistsException("no");
        doThrow(exception).when(delegate).putUnlessExists(anyLong(), anyLong());
        assertThatThrownBy(() -> preStartHandlingService.putUnlessExists(START_TIMESTAMP, COMMIT_TIMESTAMP))
                .isEqualTo(exception);
        verify(delegate).putUnlessExists(START_TIMESTAMP, COMMIT_TIMESTAMP);
    }

    @Test
    public void throwsIfTryingToPutUnlessExistsInvalidTimestamp() {
        assertThatThrownBy(() -> preStartHandlingService.putUnlessExists(NEGATIVE_TIMESTAMP, COMMIT_TIMESTAMP))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Attempted to putUnlessExists")
                .hasMessageContaining("is disallowed");
    }
}
