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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PreStartHandlingTransactionServiceTest {
    private final TransactionService delegate = mock(TransactionService.class);
    private final TransactionService preStartHandlingService = new PreStartHandlingTransactionService(delegate);

    private final long START_TIMESTAMP = 44L;
    private final long COMMIT_TIMESTAMP = 88L;
    private final long UNCOMMITTED_START_TIMESTAMP = 999L;
    private final long ZERO_TIMESTAMP = 0L;
    private final long NEGATIVE_TIMESTAMP = -125L;

    @Before
    public void setUpMocks() {
        when(delegate.get(START_TIMESTAMP)).thenReturn(COMMIT_TIMESTAMP);
        when(delegate.get(UNCOMMITTED_START_TIMESTAMP)).thenReturn(null);
    }

    @After
    public void verifyMocks() {
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void passesThroughGetsOnValidCommittedTimestamp() {
        Long timestamp = preStartHandlingService.get(START_TIMESTAMP);
        assertThat(timestamp).isEqualTo(COMMIT_TIMESTAMP);
        verify(delegate).get(START_TIMESTAMP);
    }

    @Test
    public void passesThroughGetsOnValidUncommittedTimestamp() {
        Long timestamp = preStartHandlingService.get(UNCOMMITTED_START_TIMESTAMP);
        assertThat(timestamp).isNull();
        verify(delegate).get(UNCOMMITTED_START_TIMESTAMP);
    }
}
