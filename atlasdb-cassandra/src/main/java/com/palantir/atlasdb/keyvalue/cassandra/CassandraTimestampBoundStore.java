/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.ConcurrentModificationException;
import java.util.UUID;

import javax.annotation.concurrent.GuardedBy;

import org.apache.cassandra.thrift.CqlResult;

import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import com.palantir.util.debug.ThreadDumps;

public final class CassandraTimestampBoundStore implements TimestampBoundStore {
    private static final long INITIAL_VALUE = 10000L;

    @GuardedBy("this")
    private long currentLimit = -1;
    private boolean startingUp = true;

    private final CassandraTimestampStore cassandraTimestampStore;

    public static TimestampBoundStore create(CassandraKeyValueService kvs) {
        CassandraTimestampStore cassandraTimestampStore = new CassandraTimestampStore(kvs);
        cassandraTimestampStore.createTimestampTable();
        return new CassandraTimestampBoundStore(cassandraTimestampStore);
    }

    private CassandraTimestampBoundStore(CassandraTimestampStore cassandraTimestampStore) {
        DebugLogger.logger.info(
                "Creating CassandraTimestampBoundStore object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.cassandraTimestampStore = cassandraTimestampStore;
    }

    protected UUID getId() {
        return cassandraTimestampStore.getId();
    }

    @Override
    public synchronized long getUpperLimit() {
        DebugLogger.logger.debug("[GET] Getting upper limit");
        long limitInDb = getBoundFromStore();
        checkValidLimit (limitInDb);
        currentLimit = limitInDb;
        DebugLogger.logger.info("[GET] Setting cached timestamp limit to {}.", currentLimit);
        return currentLimit;
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        throwIfStartingUp();
        checkValidLimit(limit);
        DebugLogger.logger.debug("[PUT] Storing upper limit of {}.", limit);
        try {
            CqlResult result = cassandraTimestampStore.
                    storeTimestampBound(TimestampBoundStoreEntry.create(currentLimit, getId()), limit);
            checkForConcurrentTimestampsAndPossiblyRetry(limit, result);
            currentLimit = limit;
        } catch (ConcurrentModificationException e) {
            throw constructMultipleServiceError(e);
        }
    }

    private long getBoundFromStore() {
        TimestampBoundStoreEntry currentEntry = cassandraTimestampStore.getUpperLimitEntry();
        currentEntry = migrateIfStartingUp(currentEntry);
        verifyId(currentEntry);
        return currentEntry.timestamp();
    }

    private TimestampBoundStoreEntry migrateIfStartingUp(TimestampBoundStoreEntry currentEntry) {
        if (startingUp) {
            startingUp = false;
            return migrate(currentEntry);
        }
        return currentEntry;
    }

    private TimestampBoundStoreEntry migrate(TimestampBoundStoreEntry entry) {
        DebugLogger.logger.info("[GET] Starting up. Attempting to migrate.");
        Long newLimit = entry.timestamp();
        if (newLimit == null) {
            DebugLogger.logger.info("[GET] Null result, setting timestamp limit to {}", INITIAL_VALUE);
            newLimit = INITIAL_VALUE;
        }
        try {
            cassandraTimestampStore.storeTimestampBound(entry, newLimit);
        } catch (ConcurrentModificationException e) {
            throw constructMultipleServiceError(e);
        }
        return TimestampBoundStoreEntry.create(newLimit, getId());
    }

    private void verifyId(TimestampBoundStoreEntry currentEntry) {
        if (!getId().equals(currentEntry.id())) {
            throw constructMultipleServiceError(new ConcurrentModificationException("ID in DB differs from expected!"));
        }
    }

    private void checkValidLimit (long newLimit) {
        if (newLimit < currentLimit) {
            DebugLogger.logger.error("The limit {} is lower than the cached limit {}", newLimit, currentLimit);
            throw new IllegalArgumentException("Cannot update to lower limit");
        }
    }

    private void throwIfStartingUp() {
        if (startingUp) {
            DebugLogger.logger.error("Cannot store upper limit before performing a successful migration first. "
                    + "Use getUpperLimit to migrate!");
            throw new IllegalStateException("Not migrated yet");
        }
    }

    private void checkForConcurrentTimestampsAndPossiblyRetry(long target, CqlResult result) {
        if (!CassandraTimestampUtils.wasOperationApplied(result)) {
            TimestampBoundStoreEntry entryInDb = CassandraTimestampUtils.getEntryFromApplicationResult(result);
            if (getId().equals(entryInDb.id()) && target >= entryInDb.timestamp()) {
                result = cassandraTimestampStore.storeTimestampBound(entryInDb, target);
            }
            if (!CassandraTimestampUtils.wasOperationApplied(result)) {
                throw constructConcurrentTimestampStoreException(entryInDb, target, getId(), result);
            }
        }
    }
    private static ConcurrentModificationException constructConcurrentTimestampStoreException(
            TimestampBoundStoreEntry expected,
            long target,
            UUID currentId,
            CqlResult result) {
        String actual = CassandraTimestampUtils.getTimestampAsStringFromApplicationResult(result);
        String expectedValueString = expected.getTimestampAsString();
        String expectedIdString = expected.getIdAsString();

        String msg = "Unable to CAS from {} to {}."
                + " Timestamp table entry changed underneath us (entry in memory: limit {}, ID {}; "
                + "stored in DB: limit {}, ID {}).";
        ConcurrentModificationException except = new ConcurrentModificationException(
                String.format(replaceBracesWithStringFormatSpecifier(msg),
                        expectedValueString,
                        target,
                        expectedValueString,
                        expectedIdString,
                        actual,
                        currentId));
        DebugLogger.logger.error(msg, expectedValueString, target,
                expectedValueString, expectedIdString, actual, currentId);
        DebugLogger.logger.error("Thread dump: {}", ThreadDumps.programmaticThreadDump());
        throw except;
    }

    private static String replaceBracesWithStringFormatSpecifier(String msg) {
        return msg.replaceAll("\\{\\}", "%s");
    }

    private MultipleRunningTimestampServiceError constructMultipleServiceError(ConcurrentModificationException ex) {
        throw new MultipleRunningTimestampServiceError(
                "CAS unsuccessful; this may indicate that another timestamp service is running against this"
                        + " cassandra keyspace, possibly caused by multiple copies of a service running without"
                        + " a configured set of leaders, or a CLI being run with an embedded timestamp service"
                        + " against an already running service.", ex);
    }
}
