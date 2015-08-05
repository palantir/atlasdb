package com.palantir.atlasdb.keyvalue.partition;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters.QuorumRequestParameters;
import com.palantir.common.proxy.DelegatingInvocationHandler;

public class FailableKeyValueServices {

    private static final Logger log = LoggerFactory.getLogger(FailableKeyValueServices.class);

    private static class Enabler {
        private boolean enabled;

        public boolean enabled() {
            return enabled;
        }

        public void enable() {
            enabled = true;
        }

        public void disable() {
            enabled = false;
        }

        public Enabler(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static boolean isMetaMethod(Method method) {
        return method.getName().contains("Meta") || method.getName().contains("Table");
    }

    public static boolean isReadMethod(Method method) {
        return !isMetaMethod(method) && method.getName().startsWith("get")
                && !method.getName().startsWith("getAllTimestamps");
    }

    public static boolean isWriteMethod(Method method) {
        return !isMetaMethod(method)
                && (method.getName().startsWith("put") || method.getName().startsWith("multiPut"));
    }

    static class ShutdownClassProxy implements DelegatingInvocationHandler {

        final Enabler enabler;
        Object delegate;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (isWriteMethod(method) || isReadMethod(method)) {
                if (!enabler.enabled()) {
                    log.warn("Failing for method " + method.getName() + " at service " + delegate);
                }
                Preconditions.checkState(enabler.enabled());
            }
            return method.invoke(delegate, args);
        }

        public static KeyValueService newFailableKeyValueService(Class<KeyValueService> interfaceClass,
                                                                 KeyValueService delegate,
                                                                 Enabler enabler) {

            return (KeyValueService) Proxy.newProxyInstance(
                    interfaceClass.getClassLoader(),
                    new Class<?>[] { interfaceClass },
                    new ShutdownClassProxy(delegate, enabler));
        }

        public ShutdownClassProxy(KeyValueService delegate, Enabler enabler) {
            this.delegate = delegate;
            this.enabler = enabler;
        }

        @Override
        public Object getDelegate() {
            return delegate;
        }
    }

    public static FailableKeyValueService wrap(final KeyValueService kvs) {
        return new FailableKeyValueService() {

            final Enabler enabler = new Enabler(true);

            @Override
            public void shutdown() {
                enabler.disable();
            }

            @Override
            public void resume() {
                enabler.enable();
            }

            @Override
            public KeyValueService get() {
                return ShutdownClassProxy.newFailableKeyValueService(
                        KeyValueService.class,
                        kvs,
                        enabler);
            }

        };
    }

    public static KeyValueService sampleFailingKeyValueService() {
        Set<FailableKeyValueService> svcs = Sets.newHashSet();
        Set<KeyValueService> rawSvcs = Sets.newHashSet();
        QuorumParameters quorumParameters = new QuorumParameters(5, 3, 3);
        for (int i = 0; i < 5; ++i) {
            FailableKeyValueService fkvs = FailableKeyValueServices.wrap(new InMemoryKeyValueService(
                    false));
            svcs.add(fkvs);
            rawSvcs.add(fkvs.get());
        }
        PartitionedKeyValueService parition = PartitionedKeyValueService.create(
                rawSvcs,
                quorumParameters);
        return ShutdownNodesProxy.newProxyInstance(parition, svcs, quorumParameters);
    }

    public static class ShutdownNodesProxy implements DelegatingInvocationHandler {

        final ArrayList<FailableKeyValueService> services;
        final QuorumParameters quorumParameters;
        final Random random = new Random(12345L);

        public static KeyValueService newProxyInstance(KeyValueService delegate,
                                                       Set<FailableKeyValueService> svcs,
                                                       QuorumParameters quorumParameters) {
            return (KeyValueService) Proxy.newProxyInstance(
                    KeyValueService.class.getClassLoader(),
                    new Class<?>[] { KeyValueService.class },
                    new ShutdownNodesProxy(delegate, svcs, quorumParameters));
        }

        private final Object delegate;

        public ShutdownNodesProxy(Object delegate,
                                  Set<FailableKeyValueService> svcs,
                                  QuorumParameters quorumParameters) {
            this.delegate = delegate;
            this.services = new ArrayList<>(svcs);
            this.quorumParameters = quorumParameters;
        }

        @Override
        public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                boolean isWrite = FailableKeyValueServices.isWriteMethod(method);
                boolean isRead = FailableKeyValueServices.isReadMethod(method);
                if (!isWrite && !isRead) {
                    return method.invoke(delegate, args);
                }
                QuorumRequestParameters parameters = null;
                if (isWrite) {
                    parameters = quorumParameters.getWriteRequestParameters();
                } else {
                    parameters = quorumParameters.getReadRequestParameters();
                }

                Set<FailableKeyValueService> servicesToFail = Sets.newHashSet();
                while (servicesToFail.size() < parameters.getFailureFactor() - 1) {
                    int index = random.nextInt(services.size());
                    servicesToFail.add(services.get(index));
                }
                log.warn("Shutting down services for method " + method.getName() + " : "
                        + Arrays.toString(servicesToFail.toArray()));
                for (FailableKeyValueService fkvs : servicesToFail) {
                    fkvs.shutdown();
                }
                Object ret = method.invoke(delegate, args);
                for (FailableKeyValueService fkvs : servicesToFail) {
                    fkvs.resume();
                }
                return ret;
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        @Override
        public Object getDelegate() {
            return delegate;
        }
    }
}
