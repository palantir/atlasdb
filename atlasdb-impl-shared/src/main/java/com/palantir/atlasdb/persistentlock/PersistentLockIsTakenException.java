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

import java.util.Optional;

public class PersistentLockIsTakenException extends Exception {
    private final Optional<LockEntry> otherLock;

    public PersistentLockIsTakenException(Optional<LockEntry> otherLock) {
        this.otherLock = otherLock;
    }

    @Override
    public String getMessage() {
        if (otherLock.isPresent()) {
            LockEntry otherLockEntry = otherLock.get();
            return "Lock " + otherLockEntry.lockName().name()
                    + " with id=" + otherLockEntry.lockId()
                    + " was already taken: " + otherLockEntry.reason();
        } else {
            return "Could not register lock";
        }
    }
}
