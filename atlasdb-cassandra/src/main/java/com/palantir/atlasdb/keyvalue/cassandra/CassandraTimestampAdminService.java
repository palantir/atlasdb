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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.ws.rs.QueryParam;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.timestamp.TimestampAdminService;

public class CassandraTimestampAdminService implements TimestampAdminService {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampAdminService.class);

    private static final ColumnMetadataDescription BOGUS_COLUMN_METADATA =
            new ColumnMetadataDescription(ImmutableList.of(
                    new NamedColumnDescription(
                            CassandraTimestampConstants.ROW_AND_COLUMN_NAME,
                            "current_max_ts",
                            ColumnValueDescription.forType(ValueType.FIXED_LONG))));

    private static final TableMetadata BOGUS_TIMESTAMP_TABLE_METADATA = new TableMetadata(
            CassandraTimestampConstants.NAME_METADATA,
            BOGUS_COLUMN_METADATA,
            ConflictHandler.IGNORE_ALL);

    private final KeyValueService rawKvs;
    private final CassandraClientPool clientPool;

    public CassandraTimestampAdminService(KeyValueService rawKvs) {
        Preconditions.checkArgument(rawKvs instanceof CassandraKeyValueService,
                String.format("CassandraTimestampAdminServices must be created with a CassandraKeyValueService, "
                        + "but %s was provided", rawKvs.getClass()));

        this.rawKvs = rawKvs;
        clientPool = ((CassandraKeyValueService) rawKvs).clientPool;
    }

    @Override
    public long getUpperBoundTimestamp() {
        return clientPool.runWithRetry(client -> {
            ColumnOrSuperColumn result = CassandraTimestampUtils.readCassandraTimestamp(client);

            if (result == null) {
                return 0L;
            }
            return PtBytes.toLong(result.getColumn().getValue());
        });
    }

    @Override
    public void fastForwardTimestamp(@QueryParam("newMinimum") long newMinimumTimestamp) {
        if (!timestampTableIsOk()) {
            repairTable();
        }
        advanceTableToTimestamp(newMinimumTimestamp);
    }

    @VisibleForTesting
    boolean timestampTableIsOk() {
        byte[] persistedMetadata = rawKvs.getMetadataForTable(AtlasDbConstants.TIMESTAMP_TABLE);
        return Arrays.equals(persistedMetadata, CassandraTimestampConstants.TIMESTAMP_TABLE_METADATA.persistToBytes());
    }

    @VisibleForTesting
    void repairTable() {
        rawKvs.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        rawKvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE,
                CassandraTimestampConstants.TIMESTAMP_TABLE_METADATA.persistToBytes());
    }

    private void advanceTableToTimestamp(long newMinimumTimestamp) {
        clientPool.runWithRetry(client -> {
            try {
                client.cas(
                        CassandraTimestampUtils.getRowName(),
                        AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                        ImmutableList.of(),
                        ImmutableList.of(CassandraTimestampUtils.makeColumn(newMinimumTimestamp)),
                        ConsistencyLevel.SERIAL,
                        ConsistencyLevel.EACH_QUORUM);
                return null;
            } catch (TException e) {
                log.error("Error occurred whilst trying to fast forward the old timestamp table!");
                throw Throwables.propagate(e);
            }
        });
    }

    @Override
    public void invalidateTimestamps() {
        rawKvs.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        rawKvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE, BOGUS_TIMESTAMP_TABLE_METADATA.persistToBytes());
        clientPool.runWithRetry(client -> {
            try {
                Mutation mutation = makeBogusMutation();
                Map<String, List<Mutation>> cfValues = ImmutableMap.of(
                        AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(), ImmutableList.of(mutation));
                client.batch_mutate(
                        ImmutableMap.of(CassandraTimestampUtils.getRowName(), cfValues),
                        ConsistencyLevel.QUORUM);
            } catch (TException e) {
                log.error("Error trying to install bogus column!");
                throw Throwables.propagate(e);
            }
            return null;
        });
    }

    private Mutation makeBogusMutation() {
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(makeBogusColumn()));
        return mutation;
    }

    @VisibleForTesting
    Column makeBogusColumn() {
        Column column = new Column();
        column.setName(CassandraTimestampUtils.getColumnName());
        column.setValue(new byte[0]);
        column.setTimestamp(CassandraTimestampConstants.CASSANDRA_TIMESTAMP);
        return column;
    }
}
