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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked") // Generics usage in mocks
public class TransactionSchemaInstallerTest {
    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private final Supplier<Optional<Integer>> versionToInstall = mock(Supplier.class);
    private final TransactionSchemaManager manager = mock(TransactionSchemaManager.class);

    // Non-final because we may need to set up mocks before hitting the factory
    private TransactionSchemaInstaller installer;

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(versionToInstall, manager);
    }

    @Test
    public void triesToInstallNewVersionIfPresent() {
        when(versionToInstall.get()).thenReturn(Optional.of(1));
        createAndStartTransactionSchemaInstaller();
        scheduler.runUntilIdle();
        verify(versionToInstall).get();
        verify(manager).tryInstallNewTransactionsSchemaVersion(1);
    }

    @Test
    public void doesNotTryToInstallAnyVersionIfAbsent() {
        when(versionToInstall.get()).thenReturn(Optional.empty());
        createAndStartTransactionSchemaInstaller();
        scheduler.runUntilIdle();
        verify(versionToInstall).get();
    }

    @Test
    public void resilientToFailuresToGetTheVersionToInstall() {
        when(versionToInstall.get())
                .thenThrow(new IllegalStateException("boo"))
                .thenReturn(Optional.of(2));
        createAndStartTransactionSchemaInstaller();
        scheduler.tick(15, TimeUnit.MINUTES); // TODO (jkong): Replace with expressions on the latency interval
        scheduler.runUntilIdle();
        verify(versionToInstall, times(2)).get();
        verify(manager).tryInstallNewTransactionsSchemaVersion(2);
    }

    @Test
    public void resilientToFailuresToTalkToTheCoordinationService() {
        when(versionToInstall.get()).thenReturn(Optional.of(1));
        when(manager.tryInstallNewTransactionsSchemaVersion(anyInt()))
                .thenThrow(new IllegalStateException("boo"))
                .thenReturn(true);
        createAndStartTransactionSchemaInstaller();
        scheduler.tick(15, TimeUnit.MINUTES); // TODO (jkong): Replace with expressions on the latency interval
        scheduler.runUntilIdle();
        verify(versionToInstall, times(2)).get();
        verify(manager, times(2)).tryInstallNewTransactionsSchemaVersion(1);
    }

    private void createAndStartTransactionSchemaInstaller() {
        installer = TransactionSchemaInstaller.createAndStart(manager, versionToInstall, scheduler);
    }
}
