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
import org.junit.BeforeClass;
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

    private static PostgresProcess process;
    private static KeyValueService kvs;

    @BeforeClass
    public static void setup() {
        PoolingDataSource dataSource;
        try {
            PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
            PostgresConfig config = PostgresConfig.defaultWithDbName(DB_NAME);
            PostgresExecutable exec = runtime.prepare(config);
            process = exec.start();
            if (!process.isProcessRunning()) {
                // on Macs, at least, the first run fails to start, but the second run reliably succeeds
                process = exec.start();
            }
            dataSource = new PoolingDataSource();
            dataSource.setDatabaseName(config.storage().dbName());
            dataSource.setPortNumber(config.net().port());
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }

        kvs = new PostgresKeyValueService(dataSource);
        kvs.initializeFromFreshInstance();
    }

    @AfterClass
    public static void shutdownEmbeddedPostgres() {
        process.stop();
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return kvs;
    }
}
