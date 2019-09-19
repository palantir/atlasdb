/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.impl;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import org.junit.Before;
import org.junit.Test;

public class AtlasDbServiceImplTest {
    private KeyValueService kvs;
    private AtlasDbServiceImpl atlasDbService;

    @Before
    public void setUp() {
        kvs = mock(KeyValueService.class);
        TransactionManager txManager = mock(TransactionManager.class);
        TableMetadataCache metadataCache = mock(TableMetadataCache.class);
        atlasDbService = new AtlasDbServiceImpl(kvs, txManager, metadataCache);
    }

    @Test
    public void shouldTruncateSystemTables() throws Exception {
        atlasDbService.truncateTable("_locks");

        TableReference tableToTruncate = TableReference.createWithEmptyNamespace("_locks");
        verify(kvs, atLeastOnce()).truncateTable(tableToTruncate);
    }

    @Test
    public void shouldTruncateNamespacedTables() throws Exception {
        atlasDbService.truncateTable("ns.table");

        TableReference tableToTruncate = TableReference.createFromFullyQualifiedName("ns.table");
        verify(kvs, atLeastOnce()).truncateTable(tableToTruncate);
    }
}
