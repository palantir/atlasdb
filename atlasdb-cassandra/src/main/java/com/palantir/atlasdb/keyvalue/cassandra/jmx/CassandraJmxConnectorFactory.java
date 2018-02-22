/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra.jmx;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;

public class CassandraJmxConnectorFactory {
    private final String host;
    private final int port;
    private final String username;
    private final String password;

    public CassandraJmxConnectorFactory(String host, int port, String username, String password) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(host));
        Preconditions.checkArgument(port > 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(username));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));

        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public JMXConnector create() {
        Map<String, Object> env = new HashMap<>();
        String[] creds = {username, password};
        env.put(JMXConnector.CREDENTIALS, creds);

        JMXServiceURL jmxServiceUrl;
        JMXConnector jmxConnector = null;
        try {
            jmxServiceUrl = new JMXServiceURL(String.format(CassandraConstants.JMX_RMI, host, port));
            jmxConnector = JMXConnectorFactory.connect(jmxServiceUrl, env);
        } catch (MalformedURLException e) {
            Throwables.propagate(e);
        } catch (IOException e) {
            Throwables.propagate(e);
        }

        return jmxConnector;
    }
}
