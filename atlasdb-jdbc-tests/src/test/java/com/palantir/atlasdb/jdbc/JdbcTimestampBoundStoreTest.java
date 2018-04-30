/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.jdbc;

import org.junit.After;

import com.palantir.atlasdb.keyvalue.jdbc.JdbcKeyValueService;
import com.palantir.atlasdb.keyvalue.jdbc.JdbcTimestampBoundStore;
import com.palantir.atlasdb.timestamp.AbstractDbTimestampBoundStoreTest;
import com.palantir.timestamp.TimestampBoundStore;

public class JdbcTimestampBoundStoreTest extends AbstractDbTimestampBoundStoreTest {
    private JdbcKeyValueService kvs;

    @After
    public void tearDown() throws Exception {
        kvs.close();
    }

    @Override
    protected TimestampBoundStore createTimestampBoundStore() {
        kvs = JdbcTests.createEmptyKvs();
        return JdbcTimestampBoundStore.create(kvs);
    }
}
