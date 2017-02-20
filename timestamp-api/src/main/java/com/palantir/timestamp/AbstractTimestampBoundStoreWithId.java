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
import java.util.function.Function;

import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.util.debug.ThreadDumps;

public abstract class AbstractTimestampBoundStoreWithId implements TimestampBoundStore{
    protected long currentLimit = -1;
    protected boolean startingUp = true;
    protected UUID id;
    protected long INITIAL_VALUE;

    public static Logger log = LoggerFactory.getLogger(TimestampBoundStore.class);

    protected AbstractTimestampBoundStoreWithId(Long initialValue) {
        id = UUID.randomUUID();
        INITIAL_VALUE = initialValue;
    }

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
                    setCurrentLimit("GET", entryInDb);
                    return currentLimit;
                }
        );
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        log.debug("[PUT] Storing upper limit of {}.", limit);
        checkAndSet(TimestampBoundStoreEntry.create(currentLimit, id),
                TimestampBoundStoreEntry.create(limit, id));
    }

    private TimestampBoundStoreEntry migrateIfStartingUp(TimestampBoundStoreEntry entryInDb) {
        if (!startingUp) {
            return entryInDb;
        }
        log.info("[GET] The service is starting up. Attempting to get timestamp bound from the DB and"
                + " resetting it with this process's ID.");
        TimestampBoundStoreEntry newEntry = TimestampBoundStoreEntry.create(
                entryInDb.getTimestampOrValue(INITIAL_VALUE), id);
        checkAndSet(entryInDb, newEntry);
        startingUp = false;
        return newEntry;
    }

    private void checkAndSet(TimestampBoundStoreEntry entryInDb, TimestampBoundStoreEntry newEntry) {
        cas(entryInDb, newEntry);
    }

    private void cas(TimestampBoundStoreEntry entryInDb, TimestampBoundStoreEntry newEntry) {
        checkLimitNotDecreasing(newEntry);
        log.info("[CAS] Trying to set upper limit from {} to {}.", entryInDb.getTimestampAsString(),
                newEntry.getTimestampAsString());
        Result result = updateEntryInDb(entryInDb, newEntry);
        if (result.isSuccess()) {
            setCurrentLimit("CAS", newEntry);
        } else {
            retryCasIfMatchingId(newEntry, result.entry());
        }
    }

    private void checkLimitNotDecreasing(TimestampBoundStoreEntry newEntry) {
        if (currentLimit > newEntry.getTimestampOrValue(null)) {
            throwNewTimestampTooSmallException(currentLimit, newEntry);
        }
    }

    private void setCurrentLimit(String type, TimestampBoundStoreEntry newEntry) {
        checkLimitNotDecreasing(newEntry);
        currentLimit = newEntry.getTimestampOrValue(null);
        log.info("[{}] Setting cached timestamp limit to {}.", type, currentLimit);
    }

    private void retryCasIfMatchingId(TimestampBoundStoreEntry newEntry, TimestampBoundStoreEntry entryInDb) {
        if (entryInDb.idMatches(id)) {
            setCurrentLimit("CAS", entryInDb);
            entryInDb = TimestampBoundStoreEntry.create(currentLimit, id);
            cas(entryInDb, newEntry);
        } else {
            throwStoringMultipleRunningTimestampServiceError(currentLimit, id, entryInDb, newEntry);
        }
    }

    private void checkMatchingId(TimestampBoundStoreEntry entryInDb) {
        if (!entryInDb.idMatches(id)) {
            throwGettingMultipleRunningTimestampServiceError(id, entryInDb);
        }
    }

    protected abstract TimestampBoundStoreEntry getStoredEntry(Object client);

    protected abstract Result updateEntryInDb(TimestampBoundStoreEntry entryInDb, TimestampBoundStoreEntry newEntry);

    protected abstract long runReturnLong(Function<?, Long> function);

    protected abstract void runWithNoReturn(Function<?, Void> function);

    public void throwGettingMultipleRunningTimestampServiceError(UUID id, TimestampBoundStoreEntry entryInDb) {
        String message = "Error getting the timestamp limit from the DB: the timestamp service ID {} in the DB"
                + " does not match this service's ID: {}. This may indicate that another timestamp service is"
                + " running against this cassandra keyspace.";
        String formattedMessage = MessageFormatter
                .arrayFormat(message, new String[]{entryInDb.getIdAsString(), id.toString()}).getMessage();
        logMessage(formattedMessage);
        throw new MultipleRunningTimestampServiceError(formattedMessage);
    }

    protected void throwStoringMultipleRunningTimestampServiceError(long currentLimit, UUID id,
            TimestampBoundStoreEntry entryInDb,
            TimestampBoundStoreEntry desiredNewEntry) {
        String message = "Unable to CAS from {} to {}. Timestamp limit changed underneath us or another timestamp"
                + " service ID detected. Limit in memory: {}, this service's ID: {}. Limit stored in DB: {},"
                + " the ID stored in DB: {}. This may indicate that another timestamp service is running against"
                + " this cassandra keyspace. This is likely caused by multiple copies of a service running without"
                + " a configured set of leaders or a CLI being run with an embedded timestamp service against"
                + " an already running service.";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                Long.toString(currentLimit),
                desiredNewEntry.getTimestampAsString(),
                Long.toString(currentLimit),
                id.toString(),
                entryInDb.getTimestampAsString(),
                entryInDb.getIdAsString()})
                .getMessage();
        logMessage(formattedMessage);
        throw new MultipleRunningTimestampServiceError(formattedMessage);
    }

    protected void throwNewTimestampTooSmallException(long currentLimit, TimestampBoundStoreEntry entryInDb) {
        String message = "Cannot set cached timestamp bound value from {} to {}. The bounds must be increasing!";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                Long.toString(currentLimit), entryInDb.getTimestampAsString()}).getMessage();
        logMessage(formattedMessage);
        throw new IllegalArgumentException(formattedMessage);
    }

    public static void logUpdateUncheckedException(TimestampBoundStoreEntry entryInDb,
            TimestampBoundStoreEntry desiredNewEntry) {
        String message = "[CAS] Error trying to set from {} to {}";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                entryInDb.getTimestampAsString(), desiredNewEntry.getTimestampAsString()}).getMessage();
        logMessage(formattedMessage);
    }

    private static void logMessage(String formattedMessage) {
        log.error("Error: {}", formattedMessage);
        log.error("Thread dump: {}", ThreadDumps.programmaticThreadDump());
    }

    @Value.Immutable
    public static abstract class Result {
        abstract boolean isSuccess();
        @Nullable abstract TimestampBoundStoreEntry entry();

        public static Result create(boolean isSuccess, TimestampBoundStoreEntry entry) {
            return ImmutableResult.builder()
                    .isSuccess(isSuccess)
                    .entry(entry)
                    .build();
        }

        public static Result success() {
            return create(true, null);
        }

        public static Result failure(TimestampBoundStoreEntry entry) {
            return create(false, entry);
        }
    }
}
