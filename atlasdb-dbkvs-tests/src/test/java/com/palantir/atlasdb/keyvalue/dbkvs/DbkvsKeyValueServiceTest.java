package com.palantir.atlasdb.keyvalue.dbkvs;

import org.junit.Ignore;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;

public class DbkvsKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {
    @Override
    protected KeyValueService getKeyValueService() {
        KeyValueService kvs = DbKvs.create(DbkvsTestSuite.POSTGRES_KVS_CONFIG);
        for (TableReference table : kvs.getAllTableNames()) {
            if (!table.getQualifiedName().equals("_metadata")) {
                kvs.dropTable(table);
            }
        }
        return kvs;
    }

    @Override
    @Ignore
    public void testGetRangeWithHistory() {
        /* Have to ignore this test as it is an unsupported operation for this KVS */
    }
}
