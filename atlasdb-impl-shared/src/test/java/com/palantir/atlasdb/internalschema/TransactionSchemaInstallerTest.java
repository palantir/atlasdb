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

package com.palantir.atlasdb.internalschema;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

@SuppressWarnings("unchecked") // Generics usage in mocks
public class TransactionSchemaInstallerTest {
    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private final Supplier<Optional<Integer>> versionToInstall = mock(Supplier.class);
    private final TransactionSchemaManager manager = mock(TransactionSchemaManager.class);

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(versionToInstall, manager);
    }

    @Test
    public void triesToInstallNewVersionIfPresent() {
        when(versionToInstall.get()).thenReturn(Optional.of(1));
        runAndWaitForPollingIntervals(0);
        verify(versionToInstall).get();
        verify(manager).tryInstallNewTransactionsSchemaVersion(1);
    }

    @Test
    public void doesNotInstallVersionsMoreFrequentlyThanPollingInterval() {
        when(versionToInstall.get()).thenReturn(Optional.of(1));
        runAndWaitForPollingIntervals(10);
        verify(versionToInstall, atMost(11)).get();
        verify(manager, atMost(11)).tryInstallNewTransactionsSchemaVersion(1);
    }

    @Test
    public void responsiveToChangesInVersionToInstall() {
        when(versionToInstall.get())
                .thenReturn(Optional.of(1))
                .thenReturn(Optional.of(2))
                .thenReturn(Optional.of(3));
        runAndWaitForPollingIntervals(2);
        verify(versionToInstall, times(3)).get();

        InOrder inOrder = Mockito.inOrder(manager);
        inOrder.verify(manager).tryInstallNewTransactionsSchemaVersion(1);
        inOrder.verify(manager).tryInstallNewTransactionsSchemaVersion(2);
        inOrder.verify(manager).tryInstallNewTransactionsSchemaVersion(3);
    }

    @Test
    public void doesNotTryToInstallAnyVersionIfAbsent() {
        when(versionToInstall.get()).thenReturn(Optional.empty());
        runAndWaitForPollingIntervals(0);
        verify(versionToInstall).get();
    }

    @Test
    public void resilientToFailuresToGetTheVersionToInstall() {
        when(versionToInstall.get()).thenThrow(new IllegalStateException("boo")).thenReturn(Optional.of(2));
        runAndWaitForPollingIntervals(1);
        verify(versionToInstall, times(2)).get();
        verify(manager).tryInstallNewTransactionsSchemaVersion(2);
    }

    @Test
    public void resilientToFailuresToTalkToTheCoordinationService() {
        when(versionToInstall.get()).thenReturn(Optional.of(1));
        when(manager.tryInstallNewTransactionsSchemaVersion(anyInt()))
                .thenThrow(new IllegalStateException("boo"))
                .thenReturn(true);
        runAndWaitForPollingIntervals(1);
        verify(versionToInstall, times(2)).get();
        verify(manager, times(2)).tryInstallNewTransactionsSchemaVersion(1);
    }

    private void runAndWaitForPollingIntervals(int intervals) {
        createAndStartTransactionSchemaInstaller();
        scheduler.tick(TransactionSchemaInstaller.POLLING_INTERVAL.toMinutes() * intervals, TimeUnit.MINUTES);
        scheduler.runUntilIdle();
    }

    private void createAndStartTransactionSchemaInstaller() {
        TransactionSchemaInstaller.createStarted(manager, versionToInstall, scheduler);
    }
}
