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
import java.util.stream.Collectors;

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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
                    cas(client, null, INITIAL_VALUE);
                    return INITIAL_VALUE;
                }
                Column column = result.getColumn();
                currentLimit = PtBytes.toLong(column.getValue());
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
                cas(client, currentLimit, limit);
                return null;
            }
        });
    }

    private void cas(Client client, Long oldVal, long newVal) {
        final CASResult result;
        try {
            DebugLogger.logger.info("[CAS] Trying to set upper limit from {} to {}.", oldVal, newVal);
            result = client.cas(
                    getRowName(),
                    AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                    oldVal == null ? ImmutableList.of() : ImmutableList.of(makeColumn(oldVal)),
                    ImmutableList.of(makeColumn(newVal)),
                    ConsistencyLevel.SERIAL,
                    ConsistencyLevel.EACH_QUORUM);
        } catch (Exception e) {
            log.error("[CAS] Error trying to set from {} to {}", oldVal, newVal, e);
            DebugLogger.logger.error("[CAS] Error trying to set from {} to {}", oldVal, newVal, e);
            throw Throwables.throwUncheckedException(e);
        }
        if (!result.isSuccess()) {
            String msg = "Unable to CAS from {} to {}."
                    + " Timestamp limit changed underneath us (limit in memory: {}, stored in DB: {})."
                    + " This may indicate that another timestamp service is running against this cassandra keyspace."
                    + " This is likely caused by multiple copies of a service running without a configured set of"
                    + " leaders or a CLI being run with an embedded timestamp service against an already running"
                    + " service.";
            MultipleRunningTimestampServiceError err = new MultipleRunningTimestampServiceError(
                    String.format(replaceBracesWithStringFormatSpecifier(msg),
                            oldVal,
                            newVal,
                            currentLimit,
                            getCurrentTimestampValues(result)));
            log.error(msg, oldVal, newVal, currentLimit, getCurrentTimestampValues(result), err);
            DebugLogger.logger.error(msg, oldVal, newVal, currentLimit, getCurrentTimestampValues(result), err);
            DebugLogger.logger.error("Thread dump: {}", ThreadDumps.programmaticThreadDump());
            throw err;
        } else {
            DebugLogger.logger.info("[CAS] Setting cached limit to {}.", newVal);
            currentLimit = newVal;
        }
    }

    private String replaceBracesWithStringFormatSpecifier(String msg) {
        return msg.replaceAll("\\{\\}", "%s");
    }

    private String getCurrentTimestampValues(CASResult result) {
        return result.current_values.stream()
                .map(Column::getValue)
                .map(PtBytes::toLong)
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    private Column makeColumn(long ts) {
        Column col = new Column();
        col.setName(getColumnName());
        col.setValue(PtBytes.toBytes(ts));
        col.setTimestamp(CASSANDRA_TIMESTAMP);
        return col;
    }

    private static byte[] getColumnName() {
        return CassandraKeyValueServices
                .makeCompositeBuffer(PtBytes.toBytes(ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP)
                .array();
    }

    private static ByteBuffer getRowName() {
        return ByteBuffer.wrap(PtBytes.toBytes(ROW_AND_COLUMN_NAME));
    }
}
