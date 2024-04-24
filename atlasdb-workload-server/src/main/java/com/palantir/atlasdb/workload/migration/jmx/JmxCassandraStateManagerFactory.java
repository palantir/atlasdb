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
import java.util.Map;

public final class JmxCassandraStateManagerFactory {
    private static final Map<String, Integer> JMX_PORTS =
            ImmutableMap.of("cassandra1", 7191, "cassandra2", 7192, "cassandra3", 7193);

    private JmxCassandraStateManagerFactory() {
        // utility, hacky but eh
    }

    public static JmxCassandraStateManager create(String host) {
        CassandraJmxConnectorFactory jmxConnectorFactory =
                new CassandraJmxConnectorFactory(host, JMX_PORTS.getOrDefault(host, 7199));
        return new JmxCassandraStateManager(jmxConnectorFactory::getJmxConnector);
    }
}
