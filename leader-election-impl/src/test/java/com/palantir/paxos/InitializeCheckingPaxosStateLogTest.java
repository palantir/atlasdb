/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.leader.NotCurrentLeaderException;
import java.io.IOException;
import org.junit.Test;

@SuppressWarnings("unchecked") // Generic mocks
public class InitializeCheckingPaxosStateLogTest {
    private static final long PAXOS_ROUND = 7L;
    private static final byte[] BYTES = new byte[] { 42 };

    private final PaxosStateLog<PaxosValue> delegate = mock(PaxosStateLog.class);
    private final PaxosStateLog<PaxosValue> initializeCheckingLog = new InitializeCheckingPaxosStateLog<>(delegate);

    @Test
    public void isInitializedReturnsFalseIfUnderlyingServiceNotInitialized() {
        when(delegate.isInitialized()).thenReturn(false);
        assertThat(initializeCheckingLog.isInitialized()).isFalse();
    }

    @Test
    public void isInitializedReturnsTrueIfUnderlyingServiceInitialized() {
        when(delegate.isInitialized()).thenReturn(true);
        assertThat(initializeCheckingLog.isInitialized()).isTrue();
    }

    @Test
    public void passesThroughOperationsWhenUnderlyingServiceInitialized() throws IOException {
        when(delegate.isInitialized()).thenReturn(true);
        when(delegate.readRound(PAXOS_ROUND)).thenReturn(BYTES);

        assertThat(initializeCheckingLog.readRound(PAXOS_ROUND)).isEqualTo(BYTES);
        verify(delegate, times(1)).readRound(PAXOS_ROUND);
    }

    @Test
    public void throwsWhenUnderlyingServiceNotInitialized() throws IOException {
        when(delegate.isInitialized()).thenReturn(false);
        when(delegate.readRound(PAXOS_ROUND)).thenReturn(BYTES);

        assertThatThrownBy(() -> initializeCheckingLog.readRound(PAXOS_ROUND))
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessageContaining("This node is not ready to serve requests yet! Please wait, or try the other"
                        + " leader nodes.");
        verify(delegate, never()).readRound(PAXOS_ROUND);
    }
}