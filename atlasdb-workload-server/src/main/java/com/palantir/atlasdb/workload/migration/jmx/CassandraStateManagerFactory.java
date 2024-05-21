/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.migration.jmx;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class CassandraStateManagerFactory {
    private static final Map<String, Integer> JMX_PORTS = ImmutableMap.<String, Integer>builder()
            .put("cassandra11", 7191)
            .put("cassandra12", 7192)
            .put("cassandra13", 7193)
            //            .put("cassandra14", 7194)
            //            .put("cassandra15", 7195)
            //            .put("cassandra16", 7196)
            .put("cassandra21", 7291)
            .put("cassandra22", 7292)
            .put("cassandra23", 7293)
            //            .put("cassandra24", 7294)
            //            .put("cassandra25", 7295)
            //            .put("cassandra26", 7296)
            .buildOrThrow();

    private static final List<String> DC2_HOSTNAMES =
            JMX_PORTS.keySet().stream().filter(x -> x.startsWith("cassandra2")).collect(Collectors.toList());

    private CassandraStateManagerFactory() {
        // utility, hacky but eh
    }

    public static CassandraStateManager create(Collection<String> hosts) {
        return new CombinedCassandraStateManager(
                hosts.stream().map(CassandraStateManagerFactory::create).collect(Collectors.toList()));
    }

    public static CassandraStateManager create(String host) {
        int port = JMX_PORTS.get(host); // Will throw if null, which is intentional
        CassandraJmxConnectorFactory jmxConnectorFactory = new CassandraJmxConnectorFactory(host, port);
        return new JmxCassandraStateManager(jmxConnectorFactory::getJmxConnector);
    }

    public static CassandraStateManager createDc2StateManager() {
        return create(DC2_HOSTNAMES);
    }
}
