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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56:atlasdb-cassandra-integration-tests/src/test/java/com/palantir/atlasdb/keyvalue/cassandra/CQLKeyValueServiceTransactionIntegrationTest.java
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;

public class CQLKeyValueServiceTransactionIntegrationTest extends AbstractTransactionTest {
=======
import com.palantir.atlasdb.sweep.AbstractSweeperTest;
>>>>>>> merge develop into perf cli branch (#820):atlasdb-cassandra-tests/src/test/java/com/palantir/atlasdb/keyvalue/cassandra/CassandraKeyValueServiceSweeperTest.java

    @Override
    protected KeyValueService getKeyValueService() {
<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56:atlasdb-cassandra-integration-tests/src/test/java/com/palantir/atlasdb/keyvalue/cassandra/CQLKeyValueServiceTransactionIntegrationTest.java
        return CQLKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CQLTestSuite.CQLKVS_CONFIG));
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

=======
        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraTestSuite.CASSANDRA_KVS_CONFIG), CassandraTestSuite.LEADER_CONFIG);
    }
>>>>>>> merge develop into perf cli branch (#820):atlasdb-cassandra-tests/src/test/java/com/palantir/atlasdb/keyvalue/cassandra/CassandraKeyValueServiceSweeperTest.java
}
