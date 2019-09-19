/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.containers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class CassandraEnvironmentTest {
    @Rule
    public final EnvironmentVariables environment = new EnvironmentVariables();

    @Test
    public void testDefaults() {
        environment.set(CassandraEnvironment.CASSANDRA_VERSION, null);
        environment.set(CassandraEnvironment.CASSANDRA_MAX_HEAP_SIZE, null);
        environment.set(CassandraEnvironment.CASSANDRA_HEAP_NEWSIZE, null);

        assertCassandraEnvironmentContains(
                CassandraEnvironment.DEFAULT_VERSION,
                CassandraEnvironment.DEFAULT_MAX_HEAP_SIZE,
                CassandraEnvironment.DEFAULT_HEAP_NEWSIZE);
    }

    @Test
    public void testNonDefaultsAreRead() {
        String version = "1.2.19";
        String maxHeapSize = "1337m";
        String heapNewsize = "42m";

        environment.set(CassandraEnvironment.CASSANDRA_VERSION, version);
        environment.set(CassandraEnvironment.CASSANDRA_MAX_HEAP_SIZE, maxHeapSize);
        environment.set(CassandraEnvironment.CASSANDRA_HEAP_NEWSIZE, heapNewsize);

        assertCassandraEnvironmentContains(version, maxHeapSize, heapNewsize);
    }

    @Test
    public void testGetVersionWhenEnvironmentSet() {
        String expectedVersion = "1.2.19";
        environment.set(CassandraEnvironment.CASSANDRA_VERSION, expectedVersion);
        String version = CassandraEnvironment.getVersion();
        assertEquals(expectedVersion, version);
    }

    @Test
    public void testGetVersionWhenEnvironmentNotSet() {
        environment.set(CassandraEnvironment.CASSANDRA_VERSION, null);
        String version = CassandraEnvironment.getVersion();
        assertEquals(CassandraEnvironment.DEFAULT_VERSION, version);
    }

    private void assertCassandraEnvironmentContains(String version, String maxHeapSize, String heapNewsize) {
        Map<String, String> variables = CassandraEnvironment.get();
        assertThat(variables.size(), is(3));
        assertEquals(version, variables.get(CassandraEnvironment.CASSANDRA_VERSION));
        assertEquals(maxHeapSize, variables.get(CassandraEnvironment.CASSANDRA_MAX_HEAP_SIZE));
        assertEquals(heapNewsize, variables.get(CassandraEnvironment.CASSANDRA_HEAP_NEWSIZE));
    }
}
