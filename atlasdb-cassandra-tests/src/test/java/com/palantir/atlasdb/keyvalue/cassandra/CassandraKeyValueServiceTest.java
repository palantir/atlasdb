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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;

public class CassandraKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {
    private KeyValueService keyValueService;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    @Before
    public void setupKVS() {
        keyValueService = getKeyValueService();
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraTestSuite.CASSANDRA_KVS_CONFIG), CassandraTestSuite.LEADER_CONFIG);
    }

    @Override
    protected boolean reverseRangesSupported() {
        return false;
    }

    @Override
    @Ignore
    public void testGetRangeWithHistory() {
        //
    }

    @Override
    @Ignore
    public void testGetAllTableNames() {
        //
    }

    @Test
    public void testCreateTableCaseInsensitive() {
        TableReference table1 = TableReference.createFromFullyQualifiedName("ns.tAbLe");
        TableReference table2 = TableReference.createFromFullyQualifiedName("ns.table");
        TableReference table3 = TableReference.createFromFullyQualifiedName("ns.TABle");
        keyValueService.createTable(table1, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(table2, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(table3, AtlasDbConstants.GENERIC_TABLE_METADATA);
        Set<TableReference> allTables = keyValueService.getAllTableNames();
        Preconditions.checkArgument(allTables.contains(table1));
        Preconditions.checkArgument(!allTables.contains(table2));
        Preconditions.checkArgument(!allTables.contains(table3));
    }

    @Test
    public void testLockLeaderCreatesLockTable() {
        CassandraKeyValueService kvs = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(
                        CassandraTestSuite.CASSANDRA_KVS_CONFIG.withKeyspace("lockLeader")),
                        CassandraTestSuite.LEADER_CONFIG);

        assertThat(kvs.getLockTable(), is(true));
    }

    @Test
    public void testNonLockLeaderDoesNotCreateLockTable() throws InterruptedException, ExecutionException, TimeoutException {
        Future async = CassandraTestTools.async(executorService, this::createKvsAsNonLockLeader);

        Thread.sleep(5*1000);

        CassandraTestTools.assertThatFutureDidNotSucceedYet(async);
    }

    private CassandraKeyValueService createKvsAsNonLockLeader() {
        return CassandraKeyValueService.create(
                    CassandraKeyValueServiceConfigManager.createSimpleManager(
                            CassandraTestSuite.CASSANDRA_KVS_CONFIG.withKeyspace("notLockLeader").withLockLeader("someone-else")),
                    CassandraTestSuite.LEADER_CONFIG);
    }

}
