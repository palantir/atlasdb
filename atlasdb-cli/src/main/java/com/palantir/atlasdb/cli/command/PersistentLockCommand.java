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
package com.palantir.atlasdb.cli.command;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.LockEntry;
import com.palantir.atlasdb.persistentlock.PersistentLock;
import com.palantir.atlasdb.persistentlock.PersistentLockIsTakenException;
import com.palantir.atlasdb.persistentlock.PersistentLockName;
import com.palantir.atlasdb.services.AtlasDbServices;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "persistent-lock", description = "Manipulate persistent locks - use with caution!")
public class PersistentLockCommand extends SingleBackendCommand {
    private static final Logger log = LoggerFactory.getLogger(PersistentLockCommand.class);

    @Option(name = {"-l", "--list"},
            description = "List all the persistent locks currently taken")
    boolean listLocks;

    @Option(name = {"-a", "--acquire"},
            description = "Name of the lock to acquire")
    String acquireLockName;

    @Option(name = {"-r", "--release"},
            description = "Name of the lock to release")
    String releaseLockName;

    @Option(name = {"-i", "--lockId"},
            description = "Long ID of the lock to release")
    String releaseLockId;

    @Override
    public boolean isOnlineRunSupported() {
        return true;
    }

    @Override
    public int execute(final AtlasDbServices services) {
        KeyValueService keyValueService = services.getKeyValueService();
        PersistentLock persistentLock = new PersistentLock(keyValueService);

        System.out.println("keyValueService = " + keyValueService);

        if (acquireLockName != null) {
            return acquireLock(persistentLock, acquireLockName);
        } else if (releaseLockName != null) {
            if (releaseLockId != null) {
                return releaseLock(persistentLock, releaseLockName, Long.parseLong(releaseLockId));
            } else {
                log.error("To release a lock, you must specify its lockId");
                return 1;
            }
        } else if (listLocks) {
            return printLockList(persistentLock);
        } else {
            log.error("See the help page for instructions on how to use this CLI");
            return 1;
        }
    }

    private int acquireLock(PersistentLock persistentLock, String lockName) {
        try {
            LockEntry acquiredLock = persistentLock.acquireLock(
                    PersistentLockName.of(lockName), "PersistentLock CLI command");
            log.info("Successfully acquired persistent lock " + acquiredLock);
            return 0;
        } catch (PersistentLockIsTakenException e) {
            log.error("Could not acquire the lock because it was already taken", e);
            return 1;
        }
    }

    private int releaseLock(PersistentLock persistentLock, String lockName, long lockId) {
        LockEntry lockToRelease = LockEntry.of(PersistentLockName.of(lockName), lockId);
        persistentLock.releaseLock(lockToRelease);
        log.info("This persistent lock is now released: " + lockToRelease);
        return 0;
    }

    private int printLockList(PersistentLock persistentLock) {
        List<LockEntry> allLockEntries = persistentLock.allLockEntries();
        log.info("The following persistent locks are currently taken out (total " + allLockEntries.size() + ")");
        for (LockEntry lockEntry : allLockEntries) {
            log.info(lockEntry.toString());
        }
        return 0;
    }
}
