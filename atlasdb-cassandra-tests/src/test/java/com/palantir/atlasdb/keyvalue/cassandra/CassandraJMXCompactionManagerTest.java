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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraJmxCompactionConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;

/**
 * All tests are JMX disabled.
 */
public class CassandraJMXCompactionManagerTest {

    public static final CassandraKeyValueServiceConfig DEFAULT = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers("localhost")
            .port(9160)
            .ssl(false)
            .replicationFactor(1)
            .jmx(ImmutableCassandraJmxCompactionConfig.builder()
                    .keystore("./security/Server_Keystore")
                    .keystorePassword("atlasserver")
                    .truststore("./security/Client_Truststore")
                    .truststorePassword("atlasclient")
                    .username("admin")
                    .password("atlasdb")
                    .build())
            .build();

    @Test
    public void testNewInstance() {
        CassandraKeyValueServiceConfig config = DEFAULT;
        // no JMX enabled
        CassandraJMXCompactionManager compactionManager = CassandraJMXCompactionManager.newInstance(config);
        assertNotNull(compactionManager);
    }


    @Test
    public void testEmptyInstance() {
        CassandraKeyValueServiceConfig config = DEFAULT;
        CassandraJMXCompactionManager compactionManager = CassandraJMXCompactionManager.newInstance(config);
        assertTrue("JMX not enabled, expected it to be empty!", compactionManager.getCompactionClients().isEmpty());
    }

    @Test
    public void testCloseWithEmptyCompactionManager() {
        CassandraKeyValueServiceConfig config = DEFAULT;
        CassandraJMXCompactionManager compactionManager = CassandraJMXCompactionManager.newInstance(config);
        // empty compaction manager should not throw exception during close()
        compactionManager.close();
    }
}
