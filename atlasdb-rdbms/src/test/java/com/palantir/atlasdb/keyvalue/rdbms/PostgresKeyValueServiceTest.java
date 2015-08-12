package com.palantir.atlasdb.keyvalue.rdbms;

import java.io.IOException;

import org.junit.AfterClass;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.common.base.Throwables;

import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

public class PostgresKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {

    private static final String DB_NAME = "test";

    final static PostgresStarter<PostgresExecutable, PostgresProcess> runtime;
    final static PostgresConfig config;
    final static PostgresExecutable exec;
    final static PostgresProcess process;
    final static PoolingDataSource dataSource;
    static {
        try {
            runtime = PostgresStarter.getDefaultInstance();
            config = PostgresConfig.defaultWithDbName(DB_NAME);
            exec = runtime.prepare(config);
            process = exec.start();
            dataSource = new PoolingDataSource();
            dataSource.setDatabaseName(config.storage().dbName());
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private KeyValueService kvs;

    @AfterClass
    public static void shutdownEmbeddedPostgres() {
        process.stop();
    }

    public static PostgresKeyValueService newEmbeddedInstance() {
        PostgresKeyValueService kvs = new PostgresKeyValueService(dataSource);
        kvs.initializeFromFreshInstance();
        return kvs;
    }

    @Override
    protected KeyValueService getKeyValueService() {
        if (kvs == null) {
            KeyValueService ret = newEmbeddedInstance();
            kvs = ret;
        }
        return kvs;
    }
}
