package com.palantir.atlasdb.keyvalue.impl.partition;

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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
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

    private static final ImmutableSet<String> READ_METHODS = ImmutableSet.<String> builder().add(
            "get").add("getRows").add("getRange").add("getRangeWithHistory").add(
            "getRangeOfTimestamps").add("getFirstBatchForRanges").build();

    private static final ImmutableSet<String> WRITE_METHODS = ImmutableSet.<String> builder().add(
            "put").add("multiPut").add("putWithTimestamps").add("putUnlessExists").build();

    public static boolean isReadMethod(Method method) {
        return READ_METHODS.contains(method.getName());
    }

    public static boolean isWriteMethod(Method method) {
        return WRITE_METHODS.contains(method.getName());
    }

    // Used to stop and start a particular kvs
    static class StoppableKvsProxy implements DelegatingInvocationHandler {

        private final Enabler enabler;
        private final KeyValueService delegate;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                // Special case for equals
                if (method.getName().equals("equals") && args.length == 1 && proxy == args[0]) {
                    return true;
                }

                // Methods that can be set to fail
                if (isWriteMethod(method) || isReadMethod(method)) {
                    if (!enabler.enabled()) {
                        log.warn("Failing for method " + method.getName() + " at service "
                                + delegate);
                    }
                    Preconditions.checkState(enabler.enabled());
                }

                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        public static KeyValueService newFailableKeyValueService(KeyValueService delegate,
                                                                 Enabler enabler) {

            return (KeyValueService) Proxy.newProxyInstance(
                    KeyValueService.class.getClassLoader(),
                    new Class<?>[] { KeyValueService.class },
                    new StoppableKvsProxy(delegate, enabler));
        }

        private StoppableKvsProxy(KeyValueService delegate, Enabler enabler) {
            this.delegate = delegate;
            this.enabler = enabler;
        }

        @Override
        public KeyValueService getDelegate() {
            return delegate;
        }
    }

    // Used to automatically stop and start some of the backing kvss on each
    // function invocation
    static class ShutdownNodesProxy implements DelegatingInvocationHandler {

        private final ArrayList<FailableKeyValueService> services;
        private final QuorumParameters quorumParameters;
        private final Random random = new Random(12345L);
        private final Object delegate;

        public static KeyValueService newProxyInstance(PartitionedKeyValueService delegate,
                                                       Set<FailableKeyValueService> svcs,
                                                       QuorumParameters quorumParameters) {
            return (KeyValueService) Proxy.newProxyInstance(
                    KeyValueService.class.getClassLoader(),
                    new Class<?>[] { KeyValueService.class },
                    new ShutdownNodesProxy(delegate, svcs, quorumParameters));
        }

        private ShutdownNodesProxy(Object delegate,
                                   Set<FailableKeyValueService> svcs,
                                   QuorumParameters quorumParameters) {
            this.delegate = delegate;
            this.services = new ArrayList<>(svcs);
            this.quorumParameters = quorumParameters;
        }

        @Override
        public synchronized Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            boolean isWrite = FailableKeyValueServices.isWriteMethod(method);
            boolean isRead = FailableKeyValueServices.isReadMethod(method);
            if (!isWrite && !isRead) {
                return method.invoke(delegate, args);
            }

            QuorumRequestParameters parameters = null;
            if (isWrite) {
                parameters = quorumParameters.getWriteRequestParameters();
            } else {
                // isRead
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

            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            } finally {
                // This will be executed even if no exception is thrown
                for (FailableKeyValueService fkvs : servicesToFail) {
                    fkvs.resume();
                }
            }
        }

        @Override
        public Object getDelegate() {
            return delegate;
        }
    }

    // Allow a kvs to be stopped and started (or disabled and enabled)
    public static FailableKeyValueService wrap(final KeyValueService kvs) {
        return new FailableKeyValueService() {

            final Enabler enabler = new Enabler(true);
            final KeyValueService proxiedKvs = StoppableKvsProxy.newFailableKeyValueService(
                    kvs,
                    enabler);

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
                return proxiedKvs;
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

}
