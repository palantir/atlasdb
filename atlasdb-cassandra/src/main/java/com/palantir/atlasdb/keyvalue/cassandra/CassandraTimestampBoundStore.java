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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public final class CassandraTimestampBoundStore implements TimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampBoundStore.class);
    /* package */ static final String ROW_AND_COLUMN_NAME = "ts";

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
    private final TimestampDao timestampDao;

    @GuardedBy("this")
    private long currentLimit = -1;
    @GuardedBy("this")
    private Throwable lastWriteException = null;

    public static TimestampBoundStore create(CassandraKeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE, TIMESTAMP_TABLE_METADATA.persistToBytes());
        CassandraClientPool clientPool = kvs.clientPool;
        Preconditions.checkNotNull(clientPool, "clientPool cannot be null");
        TimestampDao dao = new CassandraTimestampDao(clientPool);
        return new CassandraTimestampBoundStore(dao);
    }

    @VisibleForTesting
    protected CassandraTimestampBoundStore(TimestampDao dao) {
        this.timestampDao = dao;
    }

    @Override
    public synchronized long getUpperLimit() {
        Optional<Long> result = timestampDao.getStoredLimit();
        if (!result.isPresent()) {
            setInitialValue();
            return INITIAL_VALUE;
        }

        System.out.println("Storing limit of " + result.get());
        currentLimit = result.get();
        return currentLimit;
    }

    private void setInitialValue() {
        cas(null, INITIAL_VALUE);
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        System.out.println(String.format("Calling cas(%d,%d)", currentLimit, limit));
        cas(currentLimit, limit);
    }

    private void cas(Long oldVal, long newVal) {
        final boolean result;
        try {
            result = timestampDao.checkAndSet(oldVal, newVal);
        } catch (Exception e) {
            lastWriteException = e;
            throw Throwables.throwUncheckedException(e);
        }
        if (!result) {
            String msg = "Timestamp limit changed underneath us (limit in memory: " + currentLimit
                    + "). This may indicate that another timestamp service is running against this cassandra keyspace."
                    + " This is likely caused by multiple copies of a service running without a configured set of"
                    + " leaders or a CLI being run with an embedded timestamp service against an already running"
                    + " service.";
            MultipleRunningTimestampServiceError err = new MultipleRunningTimestampServiceError(msg);
            log.error(msg, err);
            lastWriteException = err;
            throw err;
        } else {
            lastWriteException = null;
            System.out.println("Storing limit of " + newVal);
            currentLimit = newVal;
        }
    }
}
