/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs;

import org.junit.Ignore;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;

public class DbkvsPostgresKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {
    @Override
    protected KeyValueService getKeyValueService() {
        KeyValueService kvs = ConnectionManagerAwareDbKvs.create(DbkvsPostgresTestSuite.getKvsConfig());
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
