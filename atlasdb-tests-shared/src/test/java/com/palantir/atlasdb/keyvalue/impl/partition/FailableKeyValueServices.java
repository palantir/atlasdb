/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters.QuorumRequestParameters;

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
    // TODO: Is this the equals implementation that we want?
    static class StoppableKvsProxy extends AbstractInvocationHandler {

        private final Enabler enabler;
        private final KeyValueService delegate;

        @Override
        public Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
            try {
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

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof StoppableKvsProxy) {
                return delegate.equals(((StoppableKvsProxy) obj).delegate);
            }
            return delegate.equals(obj);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public String toString() {
            return "StoppableKvsProxy [enabler=" + enabler + ", delegate=" + delegate + "]";
        }

        public static KeyValueService newFailableKeyValueService(KeyValueService delegate,
                                                                 Enabler enabler) {
            return (KeyValueService) Proxy.newProxyInstance(
                    KeyValueService.class.getClassLoader(),
                    new Class<?>[] { KeyValueService.class },
                    new StoppableKvsProxy(delegate, enabler));
        }

        private StoppableKvsProxy(KeyValueService delegate, Enabler enabler) {
            this.delegate = Preconditions.checkNotNull(delegate);
            this.enabler = enabler;
        }
    }

    // Used to automatically stop and start some of the backing kvss on each
    // function invocation
    // TODO: Use AbstractInvocationHandler?
    static class ShutdownNodesProxy extends AbstractInvocationHandler {

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
        public synchronized Object handleInvocation(Object proxy, Method method, Object[] args)
                throws Throwable {

            boolean isWrite = isWriteMethod(method);
            boolean isRead = isReadMethod(method);

            if (!isWrite && !isRead) {
                return method.invoke(delegate, args);
            }

            final QuorumRequestParameters parameters;
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
//        PartitionedKeyValueService parition = PartitionedKeyValueService.create(
//                rawSvcs,
//                quorumParameters);
        // TODO:
        PartitionedKeyValueService parition = null;
        return ShutdownNodesProxy.newProxyInstance(parition, svcs, quorumParameters);
    }

}
