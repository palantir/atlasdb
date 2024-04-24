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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.IOException;
import java.util.Map;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.immutables.value.Value;

public class CassandraJmxConnectorFactory {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraJmxConnectorFactory.class);

    private final int jmxPort;
    private final String jmxHost;

    public CassandraJmxConnectorFactory(String host, int jmxPort) {
        this.jmxPort = jmxPort;
        this.jmxHost = host;
    }

    public CassandraJmxConnector getJmxConnector() {
        CassandraJmxConnectorParams params = getParams();
        return new CassandraJmxConnector(createJmxConnector(params));
    }

    @VisibleForTesting
    @SuppressWarnings("BanJNDI")
    JMXConnector createJmxConnector(CassandraJmxConnectorParams params) {
        String host = params.host();
        Integer port = params.jmxPort();
        try {
            JMXServiceURL jmxServiceUrl =
                    new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", host, port));
            return JMXConnectorFactory.connect(jmxServiceUrl, Map.of());
        } catch (RuntimeException | IOException e) {
            log.info("Issue creating JMX connection", e);
            throw new RuntimeException(e);
        }
    }

    private CassandraJmxConnectorParams getParams() {
        CassandraJmxConnectorParams.Builder params =
                CassandraJmxConnectorParams.builder().host(jmxHost).jmxPort(jmxPort);
        return params.build();
    }

    @Value.Immutable
    public interface CassandraJmxConnectorParams {
        String host();

        Integer jmxPort();

        static Builder builder() {
            return new Builder();
        }

        class Builder extends ImmutableCassandraJmxConnectorParams.Builder {}
    }
}
