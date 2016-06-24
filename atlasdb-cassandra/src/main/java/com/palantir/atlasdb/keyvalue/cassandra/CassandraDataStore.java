package com.palantir.atlasdb.keyvalue.cassandra;

import static com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.internalTableName;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CassandraDataStore {
    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPool clientPool;

    public CassandraDataStore(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool) {
        this.config = config;
        this.clientPool = clientPool;
    }

    public void createTable(String tableName) throws TException {
        clientPool.run(client -> {
            createTableInternal(client, TableReference.createWithEmptyNamespace(tableName));
            return null;
        });
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
}
