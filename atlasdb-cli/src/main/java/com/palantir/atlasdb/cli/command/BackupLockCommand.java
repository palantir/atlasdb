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

import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.DeletionLock;
import com.palantir.atlasdb.persistentlock.LockEntry;
import com.palantir.atlasdb.persistentlock.PersistentLock;
import com.palantir.atlasdb.persistentlock.PersistentLockIsTakenException;
import com.palantir.atlasdb.services.AtlasDbServices;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "backup-lock", description = "Acquire or release the backup lock. Used to to prevent deletions")
public class BackupLockCommand extends SingleBackendCommand {
    private static final OutputPrinter log = new OutputPrinter(LoggerFactory.getLogger(BackupLockCommand.class));

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

        if (!exactlyOneParameterIsSet(acquireLock, releaseLock)) {
            log.error("Specify one of --acquire or --release");
            return 1;
        }

        if (acquireLock) {
            return acquireLock(persistentLock);
        } else if (releaseLock) {
            return releaseLock(persistentLock);
        }
        throw new IllegalStateException("Should have hit one of the cases");
    }

    private int acquireLock(PersistentLock persistentLock) {
        try {
            LockEntry acquiredLock = persistentLock.acquireLock(
                    DeletionLock.DELETION_LOCK_NAME, "backup lock CLI");
            log.info("Successfully acquired deletion lock {}", acquiredLock);
            return 0;
        } catch (PersistentLockIsTakenException e) {
            log.error("Could not acquire the lock because it was already taken", e);
            return 1;
        }
    }

    private int releaseLock(PersistentLock persistentLock) {
        LockEntry releasedLock = persistentLock.releaseOnlyLock(DeletionLock.DELETION_LOCK_NAME);
        log.info("This persistent lock is now released: {}", releasedLock);
        return 0;
    }
}
