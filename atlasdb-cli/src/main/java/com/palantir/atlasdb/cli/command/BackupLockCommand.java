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

import java.util.Set;

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

@Command(name = "backup-lock", description = "Acquire or release the backup lock. Used to to prevent deletions")
public class BackupLockCommand extends SingleBackendCommand {
    private static final Logger log = LoggerFactory.getLogger(BackupLockCommand.class);

    @Option(name = {"-a", "--acquire"},
            description = "Acquire a backup lock")
    boolean acquireLock;

    @Option(name = {"-r", "--release"},
            description = "Name of the lock to release")
    boolean releaseLock;

    @Override
    public boolean isOnlineRunSupported() {
        return true;
    }

    @Override
    public int execute(final AtlasDbServices services) {
        KeyValueService keyValueService = services.getKeyValueService();
        PersistentLock persistentLock = PersistentLock.create(keyValueService);

        if (acquireLock) {
            return acquireLock(persistentLock);
        } else if (releaseLock) {
            return releaseLock(persistentLock);
        } else {
            log.error("Specify one of --acquire or --release");
            return 1;
        }
    }

    private int acquireLock(PersistentLock persistentLock, String lockName) {
        try {
            LockEntry acquiredLock = persistentLock.acquireLock(
                    PersistentLockName.of(lockName), "PersistentLock CLI command");
            log.info("Successfully acquired persistent lock {}", acquiredLock);
            return 0;
        } catch (PersistentLockIsTakenException e) {
            log.error("Could not acquire the lock because it was already taken", e);
            return 1;
        }
    }

    private int releaseLock(PersistentLock persistentLock, String lockName, long lockId) {
        LockEntry lockToRelease = LockEntry.of(PersistentLockName.of(lockName), lockId);
        persistentLock.releaseLock(lockToRelease);
        log.info("This persistent lock is now released: {}", lockToRelease);
        return 0;
    }

    private int printLockList(PersistentLock persistentLock) {
        Set<LockEntry> allLockEntries = persistentLock.allLockEntries();
        log.info("The following persistent locks are currently taken out (total {})", allLockEntries.size());
        for (LockEntry lockEntry : allLockEntries) {
            log.info(" - {}", lockEntry.toString());
        }
        return 0;
    }
}
