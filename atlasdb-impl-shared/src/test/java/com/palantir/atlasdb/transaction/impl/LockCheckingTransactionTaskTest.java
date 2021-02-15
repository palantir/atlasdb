/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class LockCheckingTransactionTaskTest {
    private final TimelockService timelockService = mock(TimelockService.class);
    private final LockToken lockToken = mock(LockToken.class);
    private final Transaction transaction = mock(Transaction.class);
    private final TransactionTask delegate = spy(new TransactionTask() {
        @Override
        public Object execute(Transaction txn) throws Exception {
            return "result";
        }
    });
    private final TransactionTask wrappingTask = new LockCheckingTransactionTask(delegate, timelockService, lockToken);

    @Before
    public void setUp() {
        Set<LockToken> lockTokens = ImmutableSet.of(lockToken);
        when(timelockService.refreshLockLeases(lockTokens)).thenReturn(lockTokens);
    }

    @Test
    public void shouldCallDelegateOnce() throws Exception {
        wrappingTask.execute(transaction);
        verify(delegate, times(1)).execute(transaction);
    }

    @Test
    public void shouldReturnResultOfDelegate() throws Exception {
        assertThat(wrappingTask.execute(transaction)).isEqualTo("result");
    }

    @Test
    public void shouldRethrowInterruptedException() throws Exception {
        Exception exception = new InterruptedException();
        when(delegate.execute(transaction)).thenThrow(exception);
        assertThatThrownBy(() -> wrappingTask.execute(transaction)).isEqualTo(exception);
    }

    @Test
    public void shouldRethrowNonRetriableException() throws Exception {
        Exception exception = new TransactionFailedNonRetriableException("msg");
        when(delegate.execute(transaction)).thenThrow(exception);
        assertThatThrownBy(() -> wrappingTask.execute(transaction)).isEqualTo(exception);
    }

    @Test
    public void shouldRethrowExceptionIfLockIsValid() throws Exception {
        Exception exception = new IllegalStateException();
        when(delegate.execute(transaction)).thenThrow(exception);
        assertThatThrownBy(() -> wrappingTask.execute(transaction)).isEqualTo(exception);
    }

    @Test
    public void shouldThrowTransactionLockTimeoutExceptionIfLockIsInvalid() throws Exception {
        Exception exception = new IllegalStateException();
        when(delegate.execute(transaction)).thenThrow(exception);

        invalidateLockTokens();
        assertThatThrownBy(() -> wrappingTask.execute(transaction)).isInstanceOf(TransactionLockTimeoutException.class);
    }

    private void invalidateLockTokens() {
        when(timelockService.refreshLockLeases(anySet())).thenReturn(ImmutableSet.of());
    }
}
