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

import java.io.Console;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.DeletionLock;
import com.palantir.atlasdb.persistentlock.PersistentLockIsTakenException;
import com.palantir.atlasdb.services.AtlasDbServices;

import io.airlift.airline.Command;

@Command(name = "backup-lock", description = "Prevent deletions during a backup")
public class BackupLockCommand extends SingleBackendCommand {
    private static final Logger log = LoggerFactory.getLogger(BackupLockCommand.class);

    @Override
    public boolean isOnlineRunSupported() {
        return true;
    }

    @Override
	public int execute(final AtlasDbServices services) {
        Console console = System.console();
        Preconditions.checkNotNull(console,
                "This CLI must be run inside an interactive console. Pipelines are not supported");

        KeyValueService keyValueService = services.getKeyValueService();
        DeletionLock deletionLock = new DeletionLock(keyValueService);

        try {
            deletionLock.runWithLock(() -> {
                blockOnConsoleInput(console);
                return null;
            }, "Backup lock CLI");
            return 0;
        } catch (PersistentLockIsTakenException e) {
            log.error("Another process is performing deletes. "
                    + "It is not safe to take a backup now, as it may become corrupted", e);
            return 1;
        }
    }

    private void blockOnConsoleInput(Console console) {
        log.info("When your backup is complete, press ENTER to release the lock");
        console.readLine();
        log.info("Will now release the lock");
    }
}
