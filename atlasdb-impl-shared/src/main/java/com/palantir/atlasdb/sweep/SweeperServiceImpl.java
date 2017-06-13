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
package com.palantir.atlasdb.sweep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweeperservice.SweeperService;

public class SweeperServiceImpl implements SweeperService {
    private static final Logger log = LoggerFactory.getLogger(SweeperService.class);
    private BackgroundSweeperImpl backgroundSweeper;
    private KeyValueService kvs;

    public SweeperServiceImpl(BackgroundSweeperImpl backgroundSweeper,
            KeyValueService kvs) {
        this.backgroundSweeper = backgroundSweeper;
        this.kvs = kvs;
    }

    public static SweeperServiceImpl create(BackgroundSweeperImpl backgroundSweeper, KeyValueService kvs) {
        return new SweeperServiceImpl(backgroundSweeper, kvs);
    }

    @Override
    public boolean sweepTable(String tableName) {
        try {
            return grabLocksAndRunForASpecificTable(tableName);
        } catch (InterruptedException e) {
            log.warn("Sweep runner for table {} was interrupted", tableName);
        }
        return false;
    }

    public boolean grabLocksAndRunForASpecificTable(String tableName) throws InterruptedException {
        TableReference tableRef = TableReference.createFromFullyQualifiedName(tableName);

        if (!kvs.getAllTableNames().contains(tableRef)) {
            log.warn("The table being asked to swept does not exist..");
            return false;
        }

        try (SweepLocks locks = backgroundSweeper.createSweepLocks()) {
            while (true) {
                try {
                    locks.lockOrRefresh();
                    if (locks.haveLocks()) {
                        backgroundSweeper.runOnceForTable(
                                new TableToSweep(tableRef, null));
                        return true;
                    } else {
                        log.debug("Skipping sweep because sweep is running elsewhere.");
                    }
                } catch (InsufficientConsistencyException e) {
                    log.warn("Could not sweep because not all nodes of the database are online.", e);
                } catch (RuntimeException e) {
                    if (backgroundSweeper.checkAndRepairTableDrop()) {
                        log.error("The table being swept by the background sweeper was dropped, moving on...");
                        return false;
                    } else {
                        log.error("The background sweep job failed unexpectedly with batch config {}"
                                + ". Attempting to continue...", e);
                    }
                } finally {
                    Thread.sleep(20 * 1000);
                }
            }
        }
    }
}

