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
package com.palantir.atlasdb.keyvalue.leveldb.impl;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;
import org.junit.Ignore;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.common.base.Throwables;

public class LevelTransactionTest extends AbstractTransactionTest {
	private DBFactory factory = null;
	private LevelDbKeyValueService db = null;

    @Override
    public void setUp() throws Exception {
		factory = LevelDbKeyValueService.getDBFactory();
        factory.destroy(new File("testdb"), new Options());
        db = LevelDbKeyValueService.create(new File("testdb"));
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
		try {
            factory.destroy(new File("testdb"), new Options());
        } catch (IOException e) {
            Throwables.throwUncheckedException(e);
        }
    }

    @Override
    @Ignore("LevelDb doesn't support GC")
    public void testNegativeTimestamps() {
        return;
    }


}
