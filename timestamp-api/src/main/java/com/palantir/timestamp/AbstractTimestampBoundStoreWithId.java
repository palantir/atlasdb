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
package com.palantir.timestamp;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.base.FunctionCheckedException;

public abstract class AbstractTimestampBoundStoreWithId implements TimestampBoundStore{
    protected long currentLimit = -1;
    protected boolean startingUp = true;
    protected UUID id;

    public static Logger log = LoggerFactory.getLogger(TimestampBoundStore.class);

    @VisibleForTesting
    public UUID getId() {
        return id;
    }

    @Override
    public synchronized long getUpperLimit() {
        log.debug("[GET] Getting upper limit");
        return runReturnLong(client -> {
                    TimestampBoundStoreEntry entryInDb = getStoredEntry(client);
                    entryInDb = migrateIfStartingUp(entryInDb);
                    checkMatchingId(entryInDb);
                    setCurrentLimit("[GET]", entryInDb);
                    return currentLimit;
                }
        );
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        log.debug("[PUT] Storing upper limit of {}.", limit);
        casWithRetry(TimestampBoundStoreEntry.create(currentLimit, id),
                TimestampBoundStoreEntry.create(limit, id));
    }

    private TimestampBoundStoreEntry migrateIfStartingUp(TimestampBoundStoreEntry entryInDb) {
        if (!startingUp) {
            return entryInDb;
        }
        log.info("[GET] The service is starting up. Attempting to get timestamp bound from the DB and"
                + " resetting it with this process's ID.");
        TimestampBoundStoreEntry newEntry = TimestampBoundStoreEntry.create(entryInDb.getTimestampOrInitialValue(), id);
        casWithRetry(entryInDb, newEntry);
        startingUp = false;
        return newEntry;
    }

    private void casWithRetry(TimestampBoundStoreEntry entryInDb, TimestampBoundStoreEntry newEntry) {
        runWithNoReturn(client -> {
            cas(client, entryInDb, newEntry);
            return null;
        });
    }

    private void cas(Object client, TimestampBoundStoreEntry entryInDb,
            TimestampBoundStoreEntry newEntry) {
        checkLimitNotDecreasing(newEntry);
        log.info("[CAS] Trying to set upper limit from {} to {}.", entryInDb.getTimestampAsString(),
                newEntry.getTimestampAsString());
        Result result = updateEntryInDb(client, entryInDb, newEntry);
        if (result.isSuccess) {
            setCurrentLimit("[CAS]", newEntry);
        } else {
            retryCasIfMatchingId(client, newEntry, result.entry);
        }
    }

    private void checkLimitNotDecreasing(TimestampBoundStoreEntry newEntry) {
        if (currentLimit > newEntry.getTimestampOrInitialValue()) {
            throwNewTimestampTooSmallException(currentLimit, newEntry);
        }
    }

    private void setCurrentLimit(String type, TimestampBoundStoreEntry newEntry) {
        checkLimitNotDecreasing(newEntry);
        currentLimit = newEntry.getTimestampOrInitialValue();
        log.info("{} Setting cached timestamp limit to {}.", type, currentLimit);
    }

    private void retryCasIfMatchingId(Object client, TimestampBoundStoreEntry newEntry,
            TimestampBoundStoreEntry entryInDb) {
        if (entryInDb.idMatches(id)) {
            setCurrentLimit("[CAS]", entryInDb);
            entryInDb = TimestampBoundStoreEntry.create(currentLimit, id);
            cas(client, entryInDb, newEntry);
        } else {
            throwStoringMultipleRunningTimestampServiceError(currentLimit, id,
                    entryInDb, newEntry);
        }
    }

    private void checkMatchingId(TimestampBoundStoreEntry entryInDb) {
        if (!entryInDb.idMatches(id)) {
            throwGettingMultipleRunningTimestampServiceError(id, entryInDb);
        }
    }

    protected abstract TimestampBoundStoreEntry getStoredEntry(Object client);

    protected abstract Result updateEntryInDb(Object client, TimestampBoundStoreEntry entryInDb,
            TimestampBoundStoreEntry newEntry);

    protected abstract long runReturnLong(FunctionCheckedException<?, Long, RuntimeException> getFunction);

    protected abstract void runWithNoReturn(FunctionCheckedException<?, Void, RuntimeException> getFunction);

    protected abstract void throwGettingMultipleRunningTimestampServiceError(UUID id,
            TimestampBoundStoreEntry entryInDb);

    protected abstract void throwStoringMultipleRunningTimestampServiceError(long currentLimit, UUID id,
            TimestampBoundStoreEntry entryInDb, TimestampBoundStoreEntry newEntry);

    protected abstract void throwNewTimestampTooSmallException(long currentLimit,
            TimestampBoundStoreEntry newEntry);

    public class Result {
        boolean isSuccess;
        TimestampBoundStoreEntry entry;

        public Result(boolean s, TimestampBoundStoreEntry e) {
            isSuccess = s;
            entry = e;
        }
    }
}
