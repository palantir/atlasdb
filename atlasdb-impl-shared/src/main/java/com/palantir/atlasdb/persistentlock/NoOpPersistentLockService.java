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

import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;

// This class is needed because some KVSs do not support checkAndSet, upon which KvsBackedPersistentLockService relies.
public class NoOpPersistentLockService implements PersistentLockService {

    @Override
    public LockEntry acquireBackupLock(String reason) {
        return null;
    }

    @Override
    public String releaseLock(LockEntry lockEntry) throws CheckAndSetException {
        return null;
    }
}
