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

import static java.util.stream.Collectors.toSet;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.FunctionCheckedException;

public class LockTableTest {
    private LockTable lockTable;
    private CassandraClientPool clientPool;
    private CassandraKeyValueServiceConfig config;

    private String electedTableName = "_locks_elected";

    @Before
    public void setup() {
        config = CassandraTestSuite.CASSANDRA_KVS_CONFIG;
        clientPool = new CassandraClientPool(config);
        lockTable = LockTable.create(config, clientPool);
    }

    @Test
    public void shouldCreateTheLockTableItSaysItHasCreated() throws Exception {
        assertThat(allPossibleLockTables(), hasItem(lockTable.getLockTable().getTablename()));
    }

    @Ignore // this test assumes a mock paxos and real cassandra. bad!
    @Test
    public void shouldReturnNameDeterminedByLeaderElector() throws Exception {
        LockTableLeaderElector leaderElector = mock(LockTableLeaderElector.class);
        // TODO have a mock cassandra that assumes this table exists
        when(leaderElector.proposeTableToBeTheCorrectOne(anyString())).thenReturn(electedTableName);

        LockTable lockTable = LockTable.create(config, clientPool, leaderElector);

        assertThat(lockTable.getLockTable().getTablename(), equalTo(electedTableName));
    }

    @Test
    public void shouldMarkElectedTableAsWinner() throws Exception {
        assertTrue(isMarkedAsWinner(this.lockTable));
    }

    private boolean isMarkedAsWinner(LockTable table) throws Exception {
        return getTableContents(table).stream()
                .filter(this::matchesElectedColumn)
                .findAny()
                .isPresent();
    }

    private List<KeySlice> getTableContents(LockTable table) throws Exception {
        return clientPool.run((FunctionCheckedException<Cassandra.Client, List<KeySlice>, Exception>)(client) -> {
            KeyRange keyRange = new KeyRange();
            keyRange.setStart_key(new byte[0]);
            keyRange.setEnd_key(new byte[0]);

            return client.get_range_slices(
                    new ColumnParent(CassandraKeyValueService.internalTableName(table.getLockTable())),
                    getTrivialSlicePredicate(),
                    keyRange,
                    ConsistencyLevel.LOCAL_QUORUM);
        });
    }

    private boolean matchesElectedColumn(KeySlice ks) {
        String rowName = new String(ks.getKey(), Charsets.UTF_8);
        Column column = ks.getColumns().stream().findAny().get().getColumn();
        String columnName = new String(CassandraKeyValueServices.decompose(column.bufferForName()).getLhSide());
        String columnValue = new String(column.getValue());

        return ("elected".equals(rowName)
                && "elected".equals(columnName)
                && "elected".equals(columnValue));
    }

    private SlicePredicate getTrivialSlicePredicate() {
        SliceRange slice = new SliceRange(ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), false, Integer.MAX_VALUE);
        final SlicePredicate pred = new SlicePredicate();
        pred.setSlice_range(slice);
        return pred;
    }
    private Set<String> allPossibleLockTables() throws Exception {
        return clientPool.run((FunctionCheckedException<Cassandra.Client, Set<String>, Exception>)(client) -> {
            KsDef ksDef = client.describe_keyspace(config.keyspace());
            return ksDef.cf_defs.stream()
                    .map(CfDef::getName)
                    .filter(this::isLockTable)
                    .collect(toSet());
        });
    }

    private boolean isLockTable(String s) {
        return s.startsWith("_locks");
    }

}