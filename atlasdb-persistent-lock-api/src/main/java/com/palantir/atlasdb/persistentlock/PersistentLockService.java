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

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * Provides endpoints for acquiring and releasing the Backup Lock. This is intended to be used by backups and sweep,
 * to ensure that only one of these operations is deleting data at once.
 */
@Path("/persistent-lock")
public interface PersistentLockService {
    /**
     * Attempt to acquire the lock.
     * Call this method before performing any destructive operations.
     * @param reason the reason for the lock, for logging purposes (e.g. "sweep")
     * @return a {@link PersistentLockId} on success that represents the unique id of the lock that has
     *   been acquired. You will need to provide this lock id when releasing the lock.
     */
    @POST // This has to be POST because we can't allow caching.
    @Path("acquire-backup-lock")
    @Produces(MediaType.APPLICATION_JSON)
    PersistentLockId acquireBackupLock(@QueryParam("reason") String reason);

    /**
     * Release a lock that you have previously acquired.
     * Call this method as soon as you no longer need the lock (e.g. because you finished deleting stuff).
     * @param lockId the {@link PersistentLockId} you were given when you called {@link #acquireBackupLock(String)}
     */
    @POST
    @Path("release-backup-lock")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    void releaseBackupLock(PersistentLockId lockId);

    /**
     * Release and clean up the schema mutation lock state.
     * Call this method if a client has lost its lock. To avoid possible data corruption due to the
     * simultaneous creation of tables by multiple clients, this endpoint must only be used when only one client
     * is live for the given keyspace.
     */
    @POST
    @Path("release-schema-mutation-lock")
    void releaseSchemaMutationLock();
}
