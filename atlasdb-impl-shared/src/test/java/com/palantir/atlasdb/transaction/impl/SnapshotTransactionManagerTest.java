/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.Test;
import org.mockito.InOrder;

import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.lock.CloseableRemoteLockService;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.InMemoryTimestampService;

public class SnapshotTransactionManagerTest {
    private final CloseableRemoteLockService closeableRemoteLockService = mock(CloseableRemoteLockService.class);
    private final Cleaner cleaner = mock(Cleaner.class);
    private final KeyValueService keyValueService = mock(KeyValueService.class);

    private final SnapshotTransactionManager snapshotTransactionManager = new SnapshotTransactionManager(
            keyValueService,
            new InMemoryTimestampService(),
            LockClient.of("lock"),
            closeableRemoteLockService,
            null,
            null,
            null,
            null,
            cleaner);

    @Test
    public void closesKeyValueServiceOnClose() {
        snapshotTransactionManager.close();
        verify(keyValueService, times(1)).close();
    }

    @Test
    public void closesCleanerOnClose() {
        snapshotTransactionManager.close();
        verify(cleaner, times(1)).close();
    }

    @Test
    public void closesCloseableLockServiceOnClosingTransactionManager() throws IOException {
        snapshotTransactionManager.close();
        verify(closeableRemoteLockService, times(1)).close();
    }

    @Test
    public void canCloseTransactionManagerWithNonCloseableLockService() {
        SnapshotTransactionManager newTransactionManager = new SnapshotTransactionManager(
                keyValueService,
                new InMemoryTimestampService(),
                LockClient.of("lock"),
                mock(RemoteLockService.class), // not closeable
                null,
                null,
                null,
                null,
                cleaner);
        newTransactionManager.close(); // should not throw
    }

    @Test
    public void cannotRegisterNullCallback() {
        assertThatThrownBy(() -> snapshotTransactionManager.registerClosingCallback(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void invokesCallbackOnClose() {
        Runnable callback = mock(Runnable.class);

        snapshotTransactionManager.registerClosingCallback(callback);
        verify(callback, never()).run();
        snapshotTransactionManager.close();
        verify(callback).run();
    }

    @Test
    public void invokesCallbacksInReverseOrderOfRegistration() {
        Runnable callback1 = mock(Runnable.class);
        Runnable callback2 = mock(Runnable.class);
        InOrder inOrder = inOrder(callback1, callback2);

        snapshotTransactionManager.registerClosingCallback(callback1);
        snapshotTransactionManager.registerClosingCallback(callback2);
        snapshotTransactionManager.close();
        inOrder.verify(callback2).run();
        inOrder.verify(callback1).run();
    }
}
