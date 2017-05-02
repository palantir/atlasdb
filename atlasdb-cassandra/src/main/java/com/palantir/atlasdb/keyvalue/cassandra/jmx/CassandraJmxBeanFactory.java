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

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class CassandraJmxBeanFactory {
    private final JMXConnector jmxConnector;

    public CassandraJmxBeanFactory(JMXConnector jmxConnector) {
        this.jmxConnector = Preconditions.checkNotNull(jmxConnector, "jmxConnector cannot be null");
    }

    public <T> T create(String name, Class<T> interfaceClass) {
        MBeanServerConnection jmxBeanServerConnection = null;
        ObjectName objectName = null;
        try {
            jmxBeanServerConnection = jmxConnector.getMBeanServerConnection();
            objectName = new ObjectName(name);
        } catch (MalformedObjectNameException | IOException e) {
            Throwables.propagate(e);
        }
        return JMX.newMBeanProxy(jmxBeanServerConnection, objectName, interfaceClass);
    }
}
