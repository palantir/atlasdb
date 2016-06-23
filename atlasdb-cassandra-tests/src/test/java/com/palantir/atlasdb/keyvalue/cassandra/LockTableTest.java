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

import java.nio.charset.StandardCharsets;
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
import org.junit.Test;

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

    @Test
    public void shouldReturnNameDeterminedByLeaderElector() throws Exception {
        LockTableLeaderElector leaderElector = mock(LockTableLeaderElector.class);
        when(leaderElector.proposeTableToBeTheCorrectOne(anyString())).thenReturn(electedTableName);

        LockTable lockTable = LockTable.create(config, clientPool, leaderElector);

        assertThat(lockTable.getLockTable().getTablename(), equalTo(electedTableName));
    }

    @Test
    public void shouldMarkElectedTableAsWinner() throws Exception {
        String createdTable = lockTable.getLockTable().getTablename();
        boolean elected = clientPool.run((FunctionCheckedException<Cassandra.Client, Boolean, Exception>)(client) -> {
            KeyRange keyRange = new KeyRange();
            keyRange.setStart_key(new byte[0]);
            keyRange.setEnd_key(new byte[0]);
            List<KeySlice> rangeSlices = client.get_range_slices(
                    new ColumnParent(createdTable),
                    getTrivialSlicePredicate(),
                    keyRange,
                    ConsistencyLevel.LOCAL_QUORUM);

            return rangeSlices.stream()
                    .filter(ks -> {
                            Column column = ks.getColumns().stream().findAny().get().getColumn();
                            String value = PtBytes.toString(column.bufferForValue().array());
                            return value.equals("elected");
                        })
                    .findAny()
                    .isPresent();
        });
        assertTrue(elected);
    }

    private SlicePredicate getTrivialSlicePredicate() {
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);

        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(sliceRange);
        return predicate;
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