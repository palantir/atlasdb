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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class ExpectationsManagerTest {
    private final DeterministicScheduler executorService = spy(new DeterministicScheduler());
    private final ExpectationsManager manager = ExpectationsManagerImpl.createStarted(executorService);

    @Mock
    private ExpectationsAwareTransaction transaction;

    @Test
    public void runnableWasScheduled() {
        verify(executorService).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void scheduledTaskInteractsWithTransaction() {
        manager.register(transaction);
        executorService.tick(ExpectationsManagerImpl.SCHEDULER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        verify(transaction).checkAndGetViolations();
    }

    @Test
    public void doubleRegisterIsRedundant() {
        manager.register(transaction);
        manager.register(transaction);
        executorService.tick(ExpectationsManagerImpl.SCHEDULER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        verify(transaction).checkAndGetViolations();
    }

    @Test
    public void scheduledTaskIsNotInterruptedByException() {
        manager.register(transaction);
        when(transaction.checkAndGetViolations()).thenThrow(new RuntimeException());
        executorService.tick(2 * ExpectationsManagerImpl.SCHEDULER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        verify(transaction, times(2)).checkAndGetViolations();
    }

    @Test
    public void unregisterLeadsToNoMoreInteractionsWithTransaction() {
        manager.register(transaction);
        executorService.tick(ExpectationsManagerImpl.SCHEDULER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        verify(transaction).checkAndGetViolations();
        manager.unregister(transaction);
        executorService.tick(ExpectationsManagerImpl.SCHEDULER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(transaction);
    }

    @Test
    public void doubleUnregisterIsRedundant() {
        manager.register(transaction);
        executorService.tick(ExpectationsManagerImpl.SCHEDULER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        verify(transaction).checkAndGetViolations();
        manager.unregister(transaction);
        manager.unregister(transaction);
        executorService.tick(ExpectationsManagerImpl.SCHEDULER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(transaction);
    }
}
