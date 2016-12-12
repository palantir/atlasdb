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
package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import java.util.Set;
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueServiceTest;

public class RocksDbKeyValueSharedTest extends AbstractKeyValueServiceTest {
    private static RocksDbKeyValueService db = null;

    @Override
    public void setUp() throws Exception {
        if (db == null) {
            db = RocksDbKeyValueService.create("testdb");
        }
        cleanup();

        super.setUp();
    }

    @Override
    protected boolean reverseRangesSupported() {
        return false;
    }

	@Override
	protected KeyValueService getKeyValueService() {
        if (db == null) {
            db = RocksDbKeyValueService.create("testdb");
        }
        return db;
	}

    @Override
    public void tearDown() throws Exception {
        cleanup();
    }

    public static void cleanup() {
        Set<TableReference> nonMetadataTables = db.getAllTableNames().stream().filter(
                tableRef -> !tableRef.getNamespace().getName().equals("default") || !tableRef.getTablename().equals(
                        "_metadata")).collect(Collectors.toSet());
        db.dropTables(nonMetadataTables);
    }
}
