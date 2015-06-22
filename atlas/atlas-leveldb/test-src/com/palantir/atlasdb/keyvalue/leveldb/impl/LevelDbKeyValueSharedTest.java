// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.leveldb.impl;

import java.io.File;

import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;

public class LevelDbKeyValueSharedTest extends AbstractAtlasDbKeyValueServiceTest {
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
    public void tearDown() throws Exception {
        super.tearDown();
		if (db != null) {
			db.close();
			db = null;
		}
		factory.destroy(new File("testdb"), new Options());
    }



}
