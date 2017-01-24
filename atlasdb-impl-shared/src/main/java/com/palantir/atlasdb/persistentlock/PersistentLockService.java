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

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

@Path("/persistent-lock")
public class PersistentLockService {
    private final LockStore lockStore;

    @VisibleForTesting
    PersistentLockService(LockStore lockStore) {
        this.lockStore = lockStore;
    }

    public static PersistentLockService create(KeyValueService kvs) {
        LockStore lockStore = LockStore.create(kvs);
        return new PersistentLockService(lockStore);
    }

    @Path("acquire")
    public LockEntry acquireLock(@PathParam("reason") String reason) throws CheckAndSetException {
        return lockStore.acquireLock(reason);
    }

    @Path("release")
    public void releaseLock(LockEntry lockEntry) throws CheckAndSetException {
        lockStore.releaseLock(lockEntry);
    }
}
