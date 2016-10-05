/**
 * Copyright 2016 Palantir Technologies
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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        CassandraKeyValueServiceTransactionIntegrationTest.class,
        CassandraKeyValueServiceSerializableTransactionIntegrationTest.class,
})
public class CassandraTransactionsSuite {
    @ClassRule
    public static final RuleChain CASSANDRA_DOCKER_SET_UP = CassandraTestSuiteUtils.CASSANDRA_DOCKER_SET_UP;

    @BeforeClass
    public static void setup() throws IOException, InterruptedException {
        CassandraTestSuiteUtils.waitUntilCassandraIsUp();
    }
}
