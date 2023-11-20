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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.test.utils.EnvironmentVariablesExtension;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class CassandraEnvironmentTest {

    @RegisterExtension
    public final EnvironmentVariablesExtension environment = new EnvironmentVariablesExtension();

    @Test
    public void testDefaults() {
        environment.remove(CassandraEnvironment.CASSANDRA_VERSION);
        environment.remove(CassandraEnvironment.CASSANDRA_MAX_HEAP_SIZE);
        environment.remove(CassandraEnvironment.CASSANDRA_HEAP_NEWSIZE);

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
        assertThat(version).isEqualTo(expectedVersion);
    }

    @Test
    public void testGetVersionWhenEnvironmentNotSet() {
        environment.remove(CassandraEnvironment.CASSANDRA_VERSION);
        String version = CassandraEnvironment.getVersion();
        assertThat(version).isEqualTo(CassandraEnvironment.DEFAULT_VERSION);
    }

    private void assertCassandraEnvironmentContains(String version, String maxHeapSize, String heapNewsize) {
        Map<String, String> variables = CassandraEnvironment.get();
        assertThat(variables).hasSize(3);
        assertThat(variables.get(CassandraEnvironment.CASSANDRA_VERSION)).isEqualTo(version);
        assertThat(variables.get(CassandraEnvironment.CASSANDRA_MAX_HEAP_SIZE)).isEqualTo(maxHeapSize);
        assertThat(variables.get(CassandraEnvironment.CASSANDRA_HEAP_NEWSIZE)).isEqualTo(heapNewsize);
    }
}
