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
package com.palantir.util;

import com.google.common.collect.Collections2;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMISocketFactory;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.ReflectionException;
import javax.management.StandardMBean;
import javax.management.StringValueExp;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public final class JMXUtils {
    private static final Logger log = LoggerFactory.getLogger(JMXUtils.class);

    private JMXUtils() {
        /* empty */
    }

    /**
     * This method allows you to get at a bean registered with the server. Just
     * provide the class of the mbean, the objectName this bean was registered
     * with and the server this bean was registered with.
     * <p>
     * The other methods in this class to register beans use
     * {@code ManagementFactory.getPlatformMBeanServer()} as the server to
     * register with.
     */
    @SuppressWarnings("cast")
    public static <T> T newMBeanProxy(final MBeanServerConnection conn,
            final ObjectName objectName, final Class<T> interfaceClass) {
        return (T) MBeanServerInvocationHandler.newProxyInstance(conn, objectName, interfaceClass, false);
    }

    /**
     * Simple method to register a bean with the server. The server used is the
     * one returned by {@code ManagementFactory.getPlatformMBeanServer()}.
     * <p>
     * This bean will remain registered until manually unregistered. If you want
     * life-cycle management, use
     * {@link #registerMBeanWeakRefPlusCatchAndLogExceptions(Object, Class, String)}
     */
    public static void registerMBeanCatchAndLogExceptions(final Object mbean,
            final String objectName) {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            final ObjectName on = new ObjectName(objectName);
            try {
                // throws exception if bean doesn't exist
                server.getMBeanInfo(on);
                server.unregisterMBean(on);
            } catch (final InstanceNotFoundException e) {
                // if the bean wasn't already there.
            }

            server.registerMBean(mbean, on);
        } catch (InstanceAlreadyExistsException e) {
            //The bean was registered concurrently; log a warning, but don't fail tests
            log.warn("Failed to register mbean for name {}", objectName, e);
        } catch (Exception e) {
            log.warn("Unexpected exception registering mbean for name {}", objectName, e);
        }
    }

    /**
     * You must retain a reference to the returned {@link DynamicMBean} for as
     * long as you wish this bean to be registered.
     * <p>
     * Some mbeans have references to some pretty big classes and we have no
     * good way to de-register these beans because these objects don't have good
     * life-cyle management.
     * <p>
     * This method will register your mbean with JMX but JMX will hold onto it
     * weakly so the large object may be GC'ed as usual.
     * <p>
     * When there is no more reference to the underlying MBean from anywhere
     * else it may be freed. If this happens, the next call to this objectName
     * will fail with an {@link IllegalStateException}. When this happens, this
     * objectName will also be unregisterd with
     * {@link #unregisterMBeanCatchAndLogExceptions(String)}
     * <p>
     * Because of the weird JMX naming conventions, this method uses
     * {@link StandardMBean} to proxy the bean as a {@link DynamicMBean} to get
     * it through the framework
     *
     * @return the DynamicMBean whose lifecycle controls how long this bean is
     *         registered. null will be returned if this bean is not regsitered
     *         correctly
     */
    public static <T> DynamicMBean registerMBeanWeakRefPlusCatchAndLogExceptions(final T mbean,
            final Class<T> clazz, final String objectName) {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            final DynamicMBean bean = new StandardMBean(mbean, clazz);
            final DynamicMBean weakMBean = new WeakMBeanHandler(objectName, bean);
            final ObjectName on = new ObjectName(objectName);
            server.registerMBean(weakMBean, on);
            return bean;
        } catch (final Exception e) {
            log.warn("Failed to register mbean for name {}", objectName, e);
            return null;
        }
    }

    public static void unregisterMBeanCatchAndLogExceptions(final String objectName) {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            final ObjectName on = new ObjectName(objectName);
            server.unregisterMBean(on);
        } catch (final Exception e) {
            log.info("Failed to unregister mbean for name {}", e);
        }
    }

    static class WeakMBeanHandler implements DynamicMBean {
        private final WeakReference<DynamicMBean> delegateRef;
        private final static ReferenceQueue<DynamicMBean> refQueue = new ReferenceQueue<DynamicMBean>();

        static {
            final Runnable task = () -> {
                while (true) {
                    try {
                        // Blocks until available, or throws InterruptedException
                        @SuppressWarnings("unchecked")
                        final KeyedWeakReference<String, DynamicMBean> ref =
                            (KeyedWeakReference<String, DynamicMBean>) refQueue.remove();
                        unregisterMBeanCatchAndLogExceptions(ref.getKey());
                    } catch (final InterruptedException e) {
                        // Stop the cleanup thread when interrupted.
                        break;
                    } catch (final Throwable t) {
                        // Any other exception should not stop operation, for daemons.
                    }
                }
            };
            final Thread t = new Thread(task);
            t.setDaemon(true);
            t.setName("WeakMBean cleanup thread");
            t.start();
        }

        public WeakMBeanHandler(final String objectName, final DynamicMBean delegate) {
            delegateRef = new KeyedWeakReference<String, DynamicMBean>(objectName, delegate, refQueue);
        }

        public DynamicMBean delegate() {
            final DynamicMBean delegate = delegateRef.get();
            if (delegate == null) {
                throw new SafeIllegalStateException("mbean is no longer valid");
            }
            return delegate;
        }

        @Override
        public Object getAttribute(final String attribute) throws AttributeNotFoundException,
                MBeanException, ReflectionException {
            return delegate().getAttribute(attribute);
        }

        @Override
        public AttributeList getAttributes(final String[] attributes) {
            return delegate().getAttributes(attributes);
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            return delegate().getMBeanInfo();
        }

        @Override
        public Object invoke(final String actionName, final Object[] params,
                final String[] signature) throws MBeanException, ReflectionException {
            return delegate().invoke(actionName, params, signature);
        }

        @Override
        public void setAttribute(final Attribute attribute) throws AttributeNotFoundException,
                InvalidAttributeValueException, MBeanException, ReflectionException {
            delegate().setAttribute(attribute);
        }

        @Override
        public AttributeList setAttributes(final AttributeList attributes) {
            return delegate().setAttributes(attributes);
        }
    }

    public enum AllowConnections {
        ALL, LOCALHOST_ONLY
    }

    /**
     * Programmatically starts a JMX server.
     *
     * @param port
     *            port server should listen on.
     * @param allowConnections
     *            allows restriction of connections to those from "localhost"
     *            only.
     */
    public static void startLocalJMXServer(final int port, final AllowConnections allowConnections)
            throws IOException, MalformedURLException, RemoteException {
        final RMISocketFactory serverSocketFactory =
            allowConnections == AllowConnections.ALL ?
                    RMISocketFactory.getDefaultSocketFactory() : new LocalhostRMIServerSocketFactory();

        LocateRegistry.createRegistry(port, RMISocketFactory.getDefaultSocketFactory(),
                serverSocketFactory);

        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:"
                + port + "/jmxrmi");
        final JMXConnectorServer rmiServer = JMXConnectorServerFactory.newJMXConnectorServer(url,
                null, mbs);
        rmiServer.start();
    }

    /**
     * {@link RMISocketFactory} which binds server sockets to "localhost" so
     * that only connections from "localhost" are accepted.
     */
    private static class LocalhostRMIServerSocketFactory extends RMISocketFactory {
        @Override
        public ServerSocket createServerSocket(final int port) throws IOException {
            return new ServerSocket(port, 0, InetAddress.getByName("localhost"));
        }

        @Override
        public Socket createSocket(final String host, final int port) throws IOException {
            return new Socket(host, port);
        }
    }

    /**
     *
     * @param <T>
     * @param mbeanClazz
     * @return proxy interfaces to all beans registered to the server implementing the class mbeanClazz.
     */
    public static <T> Iterable<T> getInstanceBeanProxies(final Class<T> mbeanClazz){
        return Collections2.transform(
                ManagementFactory.getPlatformMBeanServer().queryNames(ObjectName.WILDCARD, Query.isInstanceOf(new StringValueExp(mbeanClazz.getName())))
                , obj -> JMXUtils.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), obj, mbeanClazz));
    }
}
