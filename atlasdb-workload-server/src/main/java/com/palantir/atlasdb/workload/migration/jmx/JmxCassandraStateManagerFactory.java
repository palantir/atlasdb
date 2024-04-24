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

public final class JmxCassandraStateManagerFactory {
    private static final int JMX_PORT = 7199;

    private JmxCassandraStateManagerFactory() {
        // utility, hacky but eh
    }

    public static JmxCassandraStateManager create(String host) {
        CassandraJmxConnectorFactory jmxConnectorFactory = new CassandraJmxConnectorFactory(host, JMX_PORT);
        return new JmxCassandraStateManager(jmxConnectorFactory::getJmxConnector);
    }
}
