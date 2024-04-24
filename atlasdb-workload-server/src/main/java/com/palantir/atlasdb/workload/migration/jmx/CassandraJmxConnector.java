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

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.IOException;
import java.util.Set;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

public class CassandraJmxConnector implements AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraJmxConnector.class);

    private final JMXConnector connector;

    public CassandraJmxConnector(JMXConnector connector) {
        this.connector = connector;
    }

    public JMXConnector getConnector() {
        return connector;
    }

    public MBeanServerConnection getMBeanServerConnection() {
        try {
            return connector.getMBeanServerConnection();
        } catch (IOException e) {
            throw new SafeIllegalStateException("Cannot get MBeanServerConnection to cassandra node", e);
        }
    }

    public <T> T getMBeanProxy(String objectName, Class<T> clazz) {
        try {
            return JMX.newMBeanProxy(connector.getMBeanServerConnection(), new ObjectName(objectName), clazz);
        } catch (IOException | MalformedObjectNameException e) {
            throw new IllegalStateException(
                    String.format(
                            "Could not create MBean proxy for class [%s] with provided JMXConnector [%s] "
                                    + "and object name [%s]",
                            clazz.getName(), connector, objectName),
                    e);
        }
    }

    public <T> T getMBeanProxy(ObjectName objectName, Class<T> clazz) {
        try {
            return JMX.newMBeanProxy(connector.getMBeanServerConnection(), objectName, clazz);
        } catch (IOException e) {
            throw new IllegalStateException(
                    String.format(
                            "Could not create MBean proxy for class [%s] with provided JMXConnector [%s] "
                                    + "and object name [%s]",
                            clazz.getName(), connector, objectName),
                    e);
        }
    }

    public <T> T getMxBeanProxy(String objectName, Class<T> clazz) {
        try {
            return JMX.newMXBeanProxy(connector.getMBeanServerConnection(), new ObjectName(objectName), clazz);
        } catch (IOException | MalformedObjectNameException e) {
            throw new IllegalStateException(
                    String.format(
                            "Could not create MXBean proxy for class [%s] with provided JMXConnector [%s] "
                                    + "and object name [%s]",
                            clazz.getName(), connector, objectName),
                    e);
        }
    }

    public Set<ObjectInstance> queryMBeans(String objectName) {
        try {
            return connector.getMBeanServerConnection().queryMBeans(new ObjectName(objectName), null);
        } catch (IOException | MalformedObjectNameException e) {
            throw new IllegalStateException(
                    String.format(
                            "Could not query MBeans for object name [%s] with provided JMXConnector [%s]",
                            objectName, connector),
                    e);
        }
    }

    @Override
    public void close() {
        try {
            connector.close();
        } catch (IOException | RuntimeException e) {
            log.warn("Encountered exception while closing the JMX connector", e);
        }
    }
}
