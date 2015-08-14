/**
 * Copyright 2015 Palantir Technologies
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

    private final static PostgresStarter<PostgresExecutable, PostgresProcess> runtime;
    private final static PostgresConfig config;
    private final static PostgresExecutable exec;
    private final static PostgresProcess process;
    private final static PoolingDataSource dataSource;
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

    private final KeyValueService kvs = newEmbeddedInstance();

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
        return kvs;
    }
}
