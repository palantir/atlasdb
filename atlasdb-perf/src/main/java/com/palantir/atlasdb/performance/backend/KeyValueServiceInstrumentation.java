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

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.spi.KeyValueServiceConfig;

public abstract class KeyValueServiceInstrumentation {

    private final int kvsPort;
    private final String dockerComposeFileName;

    private static final Logger log = LoggerFactory.getLogger(KeyValueServiceInstrumentation.class);

    private static final Map<String, KeyValueServiceInstrumentation> backendMap =
            new TreeMap<>();
    private static final Map<String, String> classNames = new TreeMap<>();

    static {
        addNewBackendType(new CassandraKeyValueServiceInstrumentation());
        addNewBackendType(new PostgresKeyValueServiceInstrumentation());
    }

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

    public static void addNewBackendType(KeyValueServiceInstrumentation backend) {
        if (!backendMap.containsKey(backend.getClassName())) {
            classNames.put(backend.toString(), backend.getClassName());
            backendMap.put(backend.getClassName(), backend);
        }
    }

    public static KeyValueServiceInstrumentation forDatabase(String backend) throws IllegalArgumentException {
        if (classNames.containsKey(backend)) {
            return backendMap.get(classNames.get(backend));
        } else {
            return forClass(backend);
        }
    }

    private static KeyValueServiceInstrumentation forClass(String className) throws IllegalArgumentException {
        if (!backendMap.containsKey(className)) {
            addBackendFromClassName(className);
        }
        return backendMap.get(className);
    }

    private static void addBackendFromClassName(String className) throws IllegalArgumentException {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor();
            KeyValueServiceInstrumentation instance = (KeyValueServiceInstrumentation) constructor.newInstance();
            addNewBackendType(instance);
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception trying to instantiate class " + className, e);
        }

    }

    public static Set<String> getBackends() {
        return classNames.keySet();
    }

    /**
     * The --backend parameter and the [dbtype] of the --db-uri parameter must match the return value of the
     * impementation of this method for your class.
     */
    public abstract String toString();

    public String getClassName() {
        return this.getClass().toString().split(" ")[1];
    }
}
