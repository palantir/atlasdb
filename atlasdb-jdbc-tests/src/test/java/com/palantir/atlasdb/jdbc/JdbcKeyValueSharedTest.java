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
package com.palantir.atlasdb.jdbc;

import org.junit.AfterClass;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.jdbc.JdbcKeyValueService;

public class JdbcKeyValueSharedTest extends AbstractKeyValueServiceTest {
    private static JdbcKeyValueService db = null;


    @Override
    public void setUp() throws Exception {
        db = JdbcTests.createEmptyKvs();
        super.setUp();
    }

    @Override
    protected boolean reverseRangesSupported() {
        return true;
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return db;
    }

    @AfterClass
    public static void tearDownKvs() {
        if (db != null) {
            db.close();
            db = null;
        }
    }
}
