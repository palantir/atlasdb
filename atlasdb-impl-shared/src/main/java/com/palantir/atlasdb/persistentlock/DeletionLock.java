/**
 * Copyright 2016 Palantir Technologies
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

import java.util.function.Supplier;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class DeletionLock {
    private final PersistentLock persistentLock;
    public static final PersistentLockName DELETION_LOCK_NAME = PersistentLockName.of("DeletionLock");

    public static DeletionLock create(KeyValueService keyValueService) {
        return new DeletionLock(PersistentLock.create(keyValueService));
    }

    private DeletionLock(PersistentLock persistentLock) {
        this.persistentLock = persistentLock;
    }

    public <T> T runWithLock(Supplier<T> supplier, String reason) throws PersistentLockIsTakenException {
        return persistentLock.runWithExclusiveLock(supplier, DELETION_LOCK_NAME, reason);
    }

    public <T> T  runWithLockNonExclusively(Supplier<T> supplier, String reason) throws PersistentLockIsTakenException {
        return persistentLock.runWithNonExclusiveLock(supplier, DELETION_LOCK_NAME, reason);
    }
}
