/*
 * Copyright 2016 Palantir Technologies
 * ​
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * ​
 * http://opensource.org/licenses/BSD-3-Clause
 * ​
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.cassandra;

import static java.util.stream.Collectors.toSet;

import static com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.internalTableName;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.TException;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.FunctionCheckedException;

public class CassandraDataStore {
    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPool clientPool;

    public CassandraDataStore(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool) {
        this.config = config;
        this.clientPool = clientPool;
    }

    public void createTable(TableReference tableReference) throws TException {
        clientPool.run(client -> {
            createTableInternal(client, tableReference);
            return null;
        });
    }

    public void put(TableReference tableReference, String rowName, String columnName, String value) {
        clientPool.run(client -> {
            try {
                byte[] colName = CassandraKeyValueServices.makeCompositeBuffer(columnName.getBytes(), 0L).array();
                Column column = new Column()
                        .setName(colName)
                        .setValue(value.getBytes())
                        .setTimestamp(0L);
                CASResult result = client.cas(
                        ByteBuffer.wrap(rowName.getBytes()),
                        tableReference.getQualifiedName(),
                        ImmutableList.of(),
                        ImmutableList.of(column),
                        ConsistencyLevel.SERIAL,
                        ConsistencyLevel.QUORUM
                );
            } catch (TException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    public boolean valueExists(TableReference tableReference, String rowName, String columnName, String value) throws Exception {
        // TODO reconsider parameter types

        return getTableContents(tableReference).stream()
                .filter(ks -> matches(ks, rowName, columnName, value))
                .findAny()
                .isPresent();
    }

    public Set<TableReference> allTables() throws Exception {
        return clientPool.run((FunctionCheckedException<Cassandra.Client, Set<TableReference>, Exception>)(client) -> {
            KsDef ksDef = client.describe_keyspace(config.keyspace());
            return ksDef.cf_defs.stream()
                    .map(CfDef::getName)
                    .map(TableReference::fromString)
                    .collect(toSet());
        });
    }

    public void removeTable(TableReference tableReference) {

    }

    // for tables internal / implementation specific to this KVS; these also don't get metadata in metadata table, nor do they show up in getTablenames, nor does this use concurrency control
    private void createTableInternal(Cassandra.Client client, TableReference tableRef) throws TException {
        if (tableAlreadyExists(client, internalTableName(tableRef))) {
            return;
        }
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), internalTableName(tableRef));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(client, tableRef.getQualifiedName(), config.schemaMutationTimeoutMillis());
    }

    private boolean tableAlreadyExists(Cassandra.Client client, String caseInsensitiveTableName) throws TException {
        KsDef ks = client.describe_keyspace(config.keyspace());
        for (CfDef cf : ks.getCf_defs()) {
            if (cf.getName().equalsIgnoreCase(caseInsensitiveTableName)) {
                return true;
            }
        }
        return false;
    }

    private List<KeySlice> getTableContents(TableReference tableRef) throws Exception {
        return clientPool.run((FunctionCheckedException<Cassandra.Client, List<KeySlice>, Exception>)(client) -> {
            KeyRange keyRange = new KeyRange();
            keyRange.setStart_key(new byte[0]);
            keyRange.setEnd_key(new byte[0]);

            return client.get_range_slices(
                    new ColumnParent(CassandraKeyValueService.internalTableName(tableRef)),
                    getTrivialSlicePredicate(),
                    keyRange,
                    ConsistencyLevel.LOCAL_QUORUM);
        });
    }

    private boolean matches(KeySlice ks, String expectedRowName, String expectedColumnName, String expectedValue) {
        String rowName = new String(ks.getKey(), Charsets.UTF_8);
        Column column = ks.getColumns().stream().findAny().get().getColumn();
        String columnName = new String(CassandraKeyValueServices.decompose(column.bufferForName()).getLhSide());
        String columnValue = new String(column.getValue());

        return (expectedRowName.equals(rowName)
                && expectedColumnName.equals(columnName)
                && expectedValue.equals(columnValue));
    }

    private SlicePredicate getTrivialSlicePredicate() {
        SliceRange slice = new SliceRange(ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), false, Integer.MAX_VALUE);
        final SlicePredicate pred = new SlicePredicate();
        pred.setSlice_range(slice);
        return pred;
    }
}
