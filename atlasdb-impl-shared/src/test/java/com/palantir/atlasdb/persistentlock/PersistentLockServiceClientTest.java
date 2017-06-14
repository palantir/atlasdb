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
package com.palantir.atlasdb.persistentlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.remoting2.clients.UserAgents;
import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.jaxrs.JaxRsClient;
import com.palantir.remoting2.servers.jersey.HttpRemotingJerseyFeature;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class PersistentLockServiceClientTest {
    private static final String REASON = "some-reason";
    private static final LockStore LOCK_STORE = LockStore.create(new InMemoryKeyValueService(true));

    @ClassRule
    public static final DropwizardClientRule DW = new DropwizardClientRule(
            new KvsBackedPersistentLockService(LOCK_STORE),
            new CheckAndSetExceptionMapper(),
            HttpRemotingJerseyFeature.DEFAULT);

    private final PersistentLockService lockService = JaxRsClient.builder().build(
            PersistentLockService.class,
            UserAgents.fromClass(KvsBackedPersistentLockServiceClientTest.class, "test", "unknown"),
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
