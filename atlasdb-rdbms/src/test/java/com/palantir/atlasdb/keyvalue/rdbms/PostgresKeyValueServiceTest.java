package com.palantir.atlasdb.keyvalue.rdbms;

import javax.sql.DataSource;

import org.postgresql.jdbc2.optional.PoolingDataSource;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;

public class PostgresKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {

    private KeyValueService kvs;

    public static DataSource getTestPostgresDataSource() {
        PoolingDataSource pgDataSource = new PoolingDataSource();
        pgDataSource.setDatabaseName("test");
        pgDataSource.setUser("test");
        pgDataSource.setPassword("password");
        return pgDataSource;
    }

    public static PostgresKeyValueService newTestInstance() {
        PostgresKeyValueService ret = new PostgresKeyValueService(getTestPostgresDataSource());
        ret.initializeFromFreshInstance();
        return ret;
    }

    @Override
    protected KeyValueService getKeyValueService() {
        if (kvs == null) {
            KeyValueService ret = new PostgresKeyValueService(getTestPostgresDataSource());
            ret.initializeFromFreshInstance();
            kvs = ret;
        }
        return kvs;
    }

}
