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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    CassandraKeyValueServiceSerializableTransactionTest.class,
    CassandraKeyValueServiceTransactionTest.class,
    CQLKeyValueServiceSerializableTransactionTest.class,
    CQLKeyValueServiceTransactionTest.class,
    CassandraTimestampTest.class,
})
public class CassandraTestSuite {

    @BeforeClass
    public static void setup() throws IOException, InterruptedException {
        CassandraService.start();
    }

    @AfterClass
    public static void stop() throws IOException {
        CassandraService.stop();
    }

}
