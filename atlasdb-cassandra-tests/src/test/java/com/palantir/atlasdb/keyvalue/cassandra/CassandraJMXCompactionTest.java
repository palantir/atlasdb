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

import static org.junit.Assert.assertNull;

import org.junit.Test;

public class CassandraJMXCompactionTest {

    @Test(timeout = 30000)
    public void testNewInstanceWithWrongPort() {
        CassandraJMXCompaction client = new CassandraJMXCompaction.Builder("127.0.0.1", 1234).build();
        assertNull(client);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInstanceWithWrongUserName() {
        new CassandraJMXCompaction.Builder("127.0.0.1", 1234).username("").build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInstanceWithWrongPassword() {
        new CassandraJMXCompaction.Builder("127.0.0.1", 1234).password("").build();
    }
}
