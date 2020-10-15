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
package com.palantir.atlasdb.persistentlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.util.TestJaxRsClientFactory;
import com.palantir.conjure.java.api.errors.RemoteException;
import com.palantir.conjure.java.server.jersey.ConjureJerseyFeature;
import io.dropwizard.testing.junit.DropwizardClientRule;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

public class KvsBackedPersistentLockServiceClientTest {
    private static final String REASON = "some-reason";
    private static final LockStoreImpl LOCK_STORE = LockStoreImpl.createImplForTest(new InMemoryKeyValueService(true));

    @ClassRule
    public static final DropwizardClientRule DW = new DropwizardClientRule(
            new KvsBackedPersistentLockService(LOCK_STORE),
            new CheckAndSetExceptionMapper(),
            ConjureJerseyFeature.INSTANCE);

    private final PersistentLockService lockService =
            TestJaxRsClientFactory.createJaxRsClientForTest(
                    PersistentLockService.class,
                    KvsBackedPersistentLockServiceClientTest.class,
                    DW.baseUri().toString());

    @After
    public void lockCleanup() {
        LOCK_STORE.allLockEntries().forEach(LOCK_STORE::releaseLock);
    }

    @Test
    public void singleAcquireReturnsLockId() {
        PersistentLockId lockId = lockService.acquireBackupLock(REASON);
        assertThat(lockId).isNotNull();
    }

    @Test
    public void singleAcquireFollowedBySingleReleaseWorks() {
        PersistentLockId lockId = lockService.acquireBackupLock(REASON);
        assertThat(lockId).isNotNull();

        lockService.releaseBackupLock(lockId);
    }

    @Test
    public void acquiresReturnConflictWhenLockIsAlreadyAcquired() {
        lockService.acquireBackupLock(REASON);

        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() -> lockService.acquireBackupLock(REASON))
                .matches(ex -> ex.getStatus() == Response.Status.CONFLICT.getStatusCode());
    }

    @Test
    public void multipleReleasesOnSameLockReturnBadRequestAfterFirstTry() {
        PersistentLockId lockId = lockService.acquireBackupLock(REASON);
        lockService.releaseBackupLock(lockId);

        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() -> lockService.releaseBackupLock(lockId))
                .matches(ex -> ex.getStatus() == Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void releaseReturnsBadRequestWithNonExistentLockId() {
        PersistentLockId nonExistentLockId = PersistentLockId.of(UUID.randomUUID());

        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() -> lockService.releaseBackupLock(nonExistentLockId))
                .matches(ex -> ex.getStatus() == Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void acquireWorksAfterReleasing() {
        PersistentLockId lockId = lockService.acquireBackupLock(REASON);

        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() -> lockService.acquireBackupLock(REASON))
                .matches(ex -> ex.getStatus() == Response.Status.CONFLICT.getStatusCode());

        lockService.releaseBackupLock(lockId);

        PersistentLockId newLockId = lockService.acquireBackupLock(REASON);
        assertThat(newLockId).isNotNull();
    }

    @Test
    public void acquireWithoutReasonReturnsBadRequest() {
        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() -> lockService.acquireBackupLock(null))
                .matches(ex -> ex.getStatus() == Response.Status.BAD_REQUEST.getStatusCode());

    }
}
