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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.GuardedBy;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import com.palantir.util.debug.ThreadDumps;

public final class CassandraTimestampBoundStore implements TimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampBoundStore.class);

    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final String ROW_AND_COLUMN_NAME = "ts";
    private static final Pattern TIMESTAMP_FORMAT_PATTERN = Pattern.compile(
            "^(?<TimestampBoundStoreId>\\d+)_(?<timestamp>\\d+)$");

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

    private final long id;
    private final CassandraClientPool clientPool;

    public static TimestampBoundStore create(CassandraKeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE, TIMESTAMP_TABLE_METADATA.persistToBytes());
        return new CassandraTimestampBoundStore(kvs.clientPool);
    }

    private CassandraTimestampBoundStore(CassandraClientPool clientPool) {
        DebugLogger.logger.info(
                "Creating CassandraTimestampBoundStore object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.clientPool = Preconditions.checkNotNull(clientPool, "clientPool cannot be null");
        this.id = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    }

    @Override
    public synchronized long getUpperLimit() {
        DebugLogger.logger.debug("[GET] Getting upper limit");
        return clientPool.runWithRetry(new FunctionCheckedException<Client, Long, RuntimeException>() {
            @Override
            public Long apply(Client client) {
                ByteBuffer rowName = getRowName();
                ColumnPath columnPath = new ColumnPath(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName());
                columnPath.setColumn(getColumnName());
                ColumnOrSuperColumn result;
                try {
                    result = client.get(rowName, columnPath, ConsistencyLevel.LOCAL_QUORUM);
                } catch (NotFoundException e) {
                    result = null;
                } catch (Exception e) {
                    throw Throwables.throwUncheckedException(e);
                }
                if (result == null) {
                    DebugLogger.logger.info("[GET] Null result, setting timestamp limit to {}", INITIAL_VALUE);
                    cas(client, makeColumnWithNewFormat(getId(), null), null, INITIAL_VALUE);
                    return INITIAL_VALUE;
                }
                Column column = result.getColumn();
                currentLimit = (new IdAndTimestamp(column)).getTimestamp();
                DebugLogger.logger.info("[GET] Setting cached timestamp limit to {}.", currentLimit);
                return currentLimit;
            }
        });
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        DebugLogger.logger.debug("[PUT] Storing upper limit of {}.", limit);
        clientPool.runWithRetry(new FunctionCheckedException<Client, Void, RuntimeException>() {
            @Override
            public Void apply(Client client) {
                cas(client, makeColumnWithNewFormat(getId(), currentLimit), currentLimit, limit);
                return null;
            }
        });
    }

    private void cas(Client client, Column oldColumn, Long oldVal, long newVal) {
        CASResult result = updateTimestampInDb(client, oldColumn, oldVal, newVal);
        if (result.isSuccess()) {
            DebugLogger.logger.info("[CAS] Setting cached limit to {}.", newVal);
            currentLimit = newVal;
        } else {
            if (result.getCurrent_values().isEmpty()) {
                log.error("[CAS] Error trying to set timestamp limit to {}: there is no limit stored in DB.", newVal);
                throw new MultipleRunningTimestampServiceError("Error trying to set timestamp limit: there is no "
                        + "limit stored in DB.");
            }
            IdAndTimestamp currentIdAndTimestamp = new IdAndTimestamp(result);
            /*
             * For the cas to succeed, the existing entry in the DB must be this.id_this.currentLimit. If that is not
             * the case, we still want to succeed if:
             *   1. id in the DB equals this.id -- indicates miscommunication about what was written to the DB,
             *   but is not a case of multiple running timestamps; or
             *   2. limit in the DB equals the expected limit, but there is no id/id does not match -- this is the case
             *   when we startup.
             */
            if (sameIdOrExpectedLimit(currentIdAndTimestamp)) {
                Column expectedColumn = getExpectedColumn(currentIdAndTimestamp);
                cas(client, expectedColumn, currentIdAndTimestamp.getTimestamp(), newVal);
            } else {
                throwMultipleRunningTimestampServiceError(oldVal, newVal, currentIdAndTimestamp);
            }
        }
    }

    private void throwMultipleRunningTimestampServiceError(Long oldVal, long newVal,
            IdAndTimestamp currentIdAndTimestamp) {
        String msg = "Unable to CAS from {} to {}."
                + " Timestamp limit changed underneath us (limit in memory: {}, stored in DB: {}). This may"
                + " indicate that another timestamp service is running against this cassandra keyspace."
                + " This is likely caused by multiple copies of a service running without a configured set of"
                + " leaders or a CLI being run with an embedded timestamp service against an already running"
                + " service. This process's ID: {}, ID in DB: {}";
        String idInDb = currentIdAndTimestamp.hasId() ? Long.toString(currentIdAndTimestamp.getId()) : "not available.";
        String fullMessage = String.format(replaceBracesWithStringFormatSpecifier(msg), oldVal, newVal,
                currentLimit, currentIdAndTimestamp.getTimestamp(), this.getId(), idInDb);

        MultipleRunningTimestampServiceError err = new MultipleRunningTimestampServiceError(fullMessage);
        log.error(fullMessage, err);
        DebugLogger.logger.error(fullMessage, err);
        DebugLogger.logger.error("Thread dump: {}", ThreadDumps.programmaticThreadDump());
        throw err;
    }

    private CASResult updateTimestampInDb(Client client, Column oldColumn, Long oldVal, long newVal) {
        CASResult result;
        DebugLogger.logger.info("[CAS] Trying to set upper limit from {} to {}.", oldVal, newVal);
        try {
            result = client.cas(
                    getRowName(),
                    AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                    oldVal == null ? ImmutableList.of() : ImmutableList.of(oldColumn),
                    ImmutableList.of(makeColumnWithNewFormat(getId(), newVal)),
                    ConsistencyLevel.SERIAL,
                    ConsistencyLevel.EACH_QUORUM);
        } catch (Exception e) {
            log.error("[CAS] Error trying to set from {} to {}", oldVal, newVal, e);
            DebugLogger.logger.error("[CAS] Error trying to set from {} to {}", oldVal, newVal, e);
            throw Throwables.throwUncheckedException(e);
        }
        return result;
    }

    private Column makeColumnWithNewFormat(Long idToUse, Long ts) {
        if (ts == null) {
            return null;
        }
        return makeColumn(PtBytes.toBytes(idToUse + "_" + ts));
    }

    private Column makeColumnWithOldFormat(long ts) {
        return makeColumn(PtBytes.toBytes(ts));
    }

    private Column makeColumn(byte[] value) {
        Column col = new Column();
        col.setName(getColumnName());
        col.setValue(value);
        col.setTimestamp(CASSANDRA_TIMESTAMP);
        return col;
    }

    private boolean sameIdOrExpectedLimit(IdAndTimestamp currentIdAndTimestamp) {
        return currentIdAndTimestamp.getTimestamp() == currentLimit
                || (currentIdAndTimestamp.hasId() && currentIdAndTimestamp.getId() == getId());
    }

    private Column getExpectedColumn(IdAndTimestamp currentIdAndTimestamp) {
        if (currentIdAndTimestamp.hasId()) {
            return makeColumnWithNewFormat(currentIdAndTimestamp.getId(), currentIdAndTimestamp.getTimestamp());
        }
        return makeColumnWithOldFormat(currentIdAndTimestamp.getTimestamp());
    }

    private static byte[] getColumnName() {
        return CassandraKeyValueServices
                .makeCompositeBuffer(PtBytes.toBytes(ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP)
                .array();
    }

    private static ByteBuffer getRowName() {
        return ByteBuffer.wrap(PtBytes.toBytes(ROW_AND_COLUMN_NAME));
    }

    private String replaceBracesWithStringFormatSpecifier(String msg) {
        return msg.replaceAll("\\{\\}", "%s");
    }

    @VisibleForTesting
    long getId() {
        return id;
    }

    private static final class IdAndTimestamp {
        private final Optional<Long> id;
        private final long timestamp;

        /*
         * Very much of an edge case, but there is theoretically a risk of a wrong interpretation, if the byte
         * representation of a long in the old format incidentally happens to match a string with one underscore
         * somewhere in it. For example,
         *   PtBytes.toString(PtBytes.toBytes(3557616313682636848L)); // returns "1_100000"
         */
        private IdAndTimestamp(byte[] values) {
            String stringValue = PtBytes.toString(values);
            Matcher columnStringMatcher = TIMESTAMP_FORMAT_PATTERN.matcher(stringValue);
            if (columnStringMatcher.matches()) {
                String[] entries = stringValue.split("_");
                this.id = Optional.of(Long.parseLong(entries[0]));
                this.timestamp = Long.parseLong(entries[1]);
            } else {
                this.id = Optional.empty();
                this.timestamp = PtBytes.toLong(values);
            }
        }

        private IdAndTimestamp(Column column) {
            this(column.getValue());
        }

        private IdAndTimestamp(CASResult result) {
            this(Iterables.getOnlyElement(result.getCurrent_values()).getValue());
        }

        private long getTimestamp() {
            return timestamp;
        }

        private boolean hasId() {
            return id.isPresent();
        }

        private long getId() {
            return id.get();
        }
    }
}
