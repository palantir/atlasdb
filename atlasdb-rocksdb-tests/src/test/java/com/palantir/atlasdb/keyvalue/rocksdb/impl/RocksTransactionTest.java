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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;

public class RocksTransactionTest extends AbstractTransactionTest {
    private RocksDbKeyValueService db = null;

    @Override
    public void setUp() throws Exception {
        db = RocksDbKeyValueService.create("testdb");
        for (TableReference table : db.getAllTableNames()) {
            if (!table.equals("default") && !table.equals("_metadata")) {
                db.dropTable(table);
            }
        }
        super.setUp();
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return db;
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

    @Override
    public void tearDown() {
        super.tearDown();
        if (db != null) {
            db.close();
            db = null;
        }
    }
}
