/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.NamespaceCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceCleaner;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class OracleNamespaceCleanerIntegrationTest {
    private static final String TABLE_NAME = "tablenameone";

    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsOracleTestSuite::createKvs);

    private KeyValueService keyValueService;
    private DbKeyValueServiceConfig dbKeyValueServiceConfig;
    private OracleDdlConfig oracleDdlConfig;
    private NamespaceCleaner namespaceCleaner;

    @Before
    public void before() {
        keyValueService = TRM.getDefaultKvs();
        dbKeyValueServiceConfig = DbKvsOracleTestSuite.getKvsConfig();
        oracleDdlConfig = (OracleDdlConfig) dbKeyValueServiceConfig.ddl();
        namespaceCleaner = new OracleNamespaceCleaner(oracleDdlConfig, dbKeyValueServiceConfig);
    }

    @Test
    public void helpMe() {
        createTable(TABLE_NAME);
        assertThat(dbKeyValueServiceConfig.namespace()).contains("HALLO");
        // namespaceCleaner.hasNamespaceSuccessfullyDropped()
    }

    private void createTable(String tableName) {
        keyValueService.createTable(getTableReference(tableName), AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    private TableReference getTableReference(String tableName) {
        return dbKeyValueServiceConfig
                .namespace()
                .map(namespace -> TableReference.create(Namespace.create(namespace), tableName))
                .orElseGet(() -> TableReference.createWithEmptyNamespace(tableName));
    }
}
