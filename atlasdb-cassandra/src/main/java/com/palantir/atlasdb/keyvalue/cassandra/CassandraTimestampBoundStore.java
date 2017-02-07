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
import java.util.Optional;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public final class CassandraTimestampBoundStore implements TimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampBoundStore.class);

    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final String ROW_AND_COLUMN_NAME = "ts";

    public static final TableMetadata TIMESTAMP_TABLE_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(
                    new NameComponentDescription("timestamp_name", ValueType.STRING))),
            new ColumnMetadataDescription(ImmutableList.of(
                new NamedColumnDescription(
                        ROW_AND_COLUMN_NAME,
                        "current_max_ts",
                        ColumnValueDescription.forType(ValueType.FIXED_LONG)))),
            ConflictHandler.IGNORE_ALL);

    private static final long INITIAL_VALUE = 10000L;

    @GuardedBy("this")
    private long currentLimit = -1;

    private final CassandraTimestampStore cassandraTimestampStore;

    public static TimestampBoundStore create(CassandraKeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE, TIMESTAMP_TABLE_METADATA.persistToBytes());
        return new CassandraTimestampBoundStore(kvs);
    }

    private CassandraTimestampBoundStore(CassandraKeyValueService kvs) {
        DebugLogger.logger.info(
                "Creating CassandraTimestampBoundStore object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.cassandraTimestampStore = new CassandraTimestampStore(kvs);
    }

    @Override
    public synchronized long getUpperLimit() {
        DebugLogger.logger.debug("[GET] Getting upper limit");
        Optional<Long> currentBound = cassandraTimestampStore.getUpperLimit();
        long boundToStore;
        if (!currentBound.isPresent()) {
            DebugLogger.logger.info("[GET] Null result, setting timestamp limit to {}", INITIAL_VALUE);
            cassandraTimestampStore.storeTimestampBound(null, INITIAL_VALUE);
            boundToStore = INITIAL_VALUE;
        } else {
            boundToStore = currentBound.get();
        }
        currentLimit = boundToStore;
        DebugLogger.logger.info("[GET] Setting cached timestamp limit to {}.", currentLimit);
        return currentLimit;
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        DebugLogger.logger.debug("[PUT] Storing upper limit of {}.", limit);
        try {
            cassandraTimestampStore.storeTimestampBound(currentLimit, limit);
        } catch (ConcurrentModificationException e) {
            throw new MultipleRunningTimestampServiceError(
                    "CAS unsuccessful; this may indicate that another timestamp service is running against this"
                            + " cassandra keyspace, possibly caused by multiple copies of a service running without"
                            + " a configured set of leaders, or a CLI being run with an embedded timestamp service"
                            + " against an already running service.", e);
        }
    }
}
