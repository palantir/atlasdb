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
package com.palantir.atlasdb.performance.backend;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.palantir.atlasdb.spi.KeyValueServiceConfig;

public abstract class KeyValueServiceInstrumentation {

    private final int kvsPort;
    private final String dockerComposeFileName;

    KeyValueServiceInstrumentation(int kvsPort, String dockerComposeFileName) {
        this.kvsPort = kvsPort;
        this.dockerComposeFileName = dockerComposeFileName;
    }

    public String getDockerComposeResourceFileName() {
        return dockerComposeFileName;
    }

    public int getKeyValueServicePort() {
        return kvsPort;
    }

    public abstract KeyValueServiceConfig getKeyValueServiceConfig(InetSocketAddress addr);
    public abstract boolean canConnect(InetSocketAddress addr);

    private static Map<String, KeyValueServiceInstrumentation> backendMap =
            new TreeMap<>();

    static {
        addNewBackendType(new CassandraKeyValueServiceInstrumentation());
        addNewBackendType(new PostgresKeyValueServiceInstrumentation());
    }

    public static KeyValueServiceInstrumentation forDatabase(String backend) {
        return backendMap.get(backend);
    }

    public static void addNewBackendType(KeyValueServiceInstrumentation backend) {
        if (!backendMap.containsKey(backend.toString())) {
            backendMap.put(backend.toString(), backend);
        }
    }

    public static Set<String> getBackends() {
        return backendMap.keySet();
    }

    public abstract String toString();
}
