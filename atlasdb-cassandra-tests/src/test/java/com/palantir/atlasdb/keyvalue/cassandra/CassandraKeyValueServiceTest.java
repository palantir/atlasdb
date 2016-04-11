package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CassandraKeyValueServiceTest {

    private KeyValueService keyValueService;

    @Before
    public void setupKVS() {
        keyValueService = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(
                        ImmutableCassandraKeyValueServiceConfig.builder()
                                .addServers(new InetSocketAddress("localhost", 9160))
                                .poolSize(20)
                                .keyspace("atlasdb")
                                .ssl(false)
                                .replicationFactor(1)
                                .mutationBatchCount(10000)
                                .mutationBatchSizeBytes(10000000)
                                .fetchBatchCount(1000)
                                .safetyDisabled(false)
                                .autoRefreshNodes(false)
                                .build()));
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

}
