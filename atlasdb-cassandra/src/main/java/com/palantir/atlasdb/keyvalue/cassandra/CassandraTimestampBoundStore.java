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

import java.util.Optional;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import com.palantir.util.debug.ThreadDumps;

public final class CassandraTimestampBoundStore implements TimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampBoundStore.class);
    private static final long INITIAL_VALUE = 10000L;

    @GuardedBy("this")
    private long currentLimit = -1;

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

    @Override
    public synchronized long getUpperLimit() {
        DebugLogger.logger.debug("[GET] Getting upper limit");
        currentLimit = getBoundFromStore();
        return currentLimit;
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        DebugLogger.logger.debug("[PUT] Storing upper limit of {}.", limit);
        attemptToStoreTimestampBound(Optional.of(currentLimit), limit);
        currentLimit = limit;
    }

    private long getBoundFromStore() {
        Optional<Long> currentBound = cassandraTimestampStore.getUpperLimit();
        if (currentBound.isPresent()) {
            DebugLogger.logger.info("[GET] Setting cached timestamp limit to {}.", currentLimit);
            return currentBound.get();
        }
        DebugLogger.logger.info("[GET] Null result, setting timestamp limit to {}", INITIAL_VALUE);
        return attemptToStoreTimestampBound(Optional.empty(), INITIAL_VALUE);
    }

    private long attemptToStoreTimestampBound(Optional<Long> expected, long target) {
        CassandraTimestampStore.StoreTimestampResult result =
                cassandraTimestampStore.storeTimestampBound(expected, target);
        if (result.successful()) {
            return target;
        }
        throw constructMultipleServiceError(result, target);
    }

    private MultipleRunningTimestampServiceError constructMultipleServiceError(
            CassandraTimestampStore.StoreTimestampResult result, long target) {
        Optional<Long> expected = result.expected();
        Optional<Long> actual = result.actual();

        String msg = "Unable to CAS from {} to {}."
                + " Timestamp limit changed underneath us (limit in memory: {}, stored in DB: {})."
                + " This may indicate that another timestamp service is running against this cassandra keyspace,"
                + " possibly caused by multiple copies of a service running without a configured set of leaders,"
                + " or a CLI being run with an embedded timestamp service against an already runnign service.";
        log.error(msg, expected, target, expected, actual);
        DebugLogger.logger.error("Thread dump: {}", ThreadDumps.programmaticThreadDump());

        return new MultipleRunningTimestampServiceError(
                MessageFormatter.arrayFormat(msg, new Object[] {expected, target, expected, actual}).getMessage());
    }
}
