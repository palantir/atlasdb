/**
 * Copyright 2017 Palantir Technologies
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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

/**
 * Provides endpoints for acquiring and releasing the Deletion Lock. This is intended to be used by backups and sweep,
 * to ensure that only one of these operations is deleting data at once.
 */
@Path("/persistent-lock")
public class PersistentLockService {
    private static final Logger log = LoggerFactory.getLogger(PersistentLockService.class);

    private final LockStore lockStore;

    @VisibleForTesting
    PersistentLockService(LockStore lockStore) {
        this.lockStore = lockStore;
    }

    public static PersistentLockService create(KeyValueService kvs) {
        LockStore lockStore = LockStore.create(kvs);
        return new PersistentLockService(lockStore);
    }

    /**
     * Attempt to acquire the lock.
     * Call this method before performing any destructive operations.
     * @param reason the reason for the lock, for logging purposes (e.g. "sweep")
     * @return a {@link LockEntry} on success. The LockEntry will contain the given reason, and a unique ID. It is
     *   essential that you retain a reference to this lock, as you will need it in order to release the lock.
     */
    @GET
    @Path("acquire")
    @Produces(MediaType.APPLICATION_JSON)
    public LockEntry acquireLock(@QueryParam("reason") String reason) {
        Preconditions.checkNotNull(reason, "Please provide a reason for acquiring the lock.");
        return lockStore.acquireLock(reason);
    }

    /**
     * Release a lock that you have previously acquired.
     * Call this method as soon as you no longer need the lock (e.g. because you finished deleting stuff).
     * @param lockEntry the {@link LockEntry} you were given when you called {@link #acquireLock(String)}
     * @return A success message if the lock was successfully released
     * @throws CheckAndSetException if there was a conflict.
     */
    @POST
    @Path("release")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String releaseLock(LockEntry lockEntry) throws CheckAndSetException {
        Preconditions.checkNotNull(lockEntry, "Please provide a LockEntry to release.");

        try {
            lockStore.releaseLock(lockEntry);
            return "The lock was released successfully.\n";
        } catch (CheckAndSetException e) {
            log.error("Failed to release the persistent lock. This means that somebody already cleared this lock. "
                    + "You should investigate this, as this means your operation didn't necessarily hold the lock when "
                    + "it should have done.", e);
            throw e;
        }
    }

}
