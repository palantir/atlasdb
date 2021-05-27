/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvsMultiSeriesTimestampPersistenceTest;
import org.junit.After;
import org.junit.Before;

public class OracleMultiSeriesTimestampPersistenceTest extends DbKvsMultiSeriesTimestampPersistenceTest {
    private ConnectionManagerAwareDbKvs kvs;

    @Before
    public void createTimestampInterfaces() {
        kvs = DbKvsOracleTestSuite.createKvs();
    }

    @After
    public void tearDown() {
        kvs.close();
    }

    @Override
    protected ConnectionManagerAwareDbKvs getKeyValueService() {
        return kvs;
    }
}
