/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.coordination;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.leader.NotCurrentLeaderException;

public class NamespaceCoordinatingProxy<T> extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(NamespaceCoordinatingProxy.class);

    private final CoordinationService coordinationService;
    private final Supplier<T> delegateSupplier;
    private final AtomicReference<T> delegateReference;
    private final String client;
    private final String localhost;
    private final ExecutorService executor;
    private volatile boolean isClosed;

    private NamespaceCoordinatingProxy(CoordinationService coordinationService, Supplier<T> delegateSupplier,
            String client, String localhost, Class<T> clazz) {
        this.coordinationService = coordinationService;
        this.delegateSupplier = delegateSupplier;
        this.client = client;
        this.localhost = localhost;

        this.delegateReference = new AtomicReference<T>();
        this.executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("namespace-coordinator").build());
    }

    @SuppressWarnings("unchecked") // We know this cast is safe.
    public static <U> U newProxyInstance(Class<U> clazz,
            Supplier<U> delegateSupplier,
            String client,
            String localhost,
            CoordinationService coordinationService) {
        NamespaceCoordinatingProxy<U> proxy = new NamespaceCoordinatingProxy<>(
                coordinationService,
                delegateSupplier,
                client,
                localhost,
                clazz);

        return (U) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class<?>[] { clazz, Closeable.class },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        Preconditions.checkState(!isClosed, "Attempted to invoke a method on a closed proxy.");
        if (method.getName().equals("close") && args.length == 0) {
            isClosed = true;
            executor.shutdownNow();
            clearDelegate();
            return null;
        }

        if (!isAssigned()) {
            markAsOutsideCluster();
        }

        // the coordination service assigned us to our client.
        T delegate = delegateReference.get();
        while (delegate == null) {
            delegateReference.compareAndSet(null, delegateSupplier.get());
            delegate = delegateReference.get();

            // TODO (jkong): Is this needed for correctness?
            if (!isAssigned()) {
                markAsOutsideCluster();
            }
        }

        // atomic read passed, so we can go ahead
        return method.invoke(delegate, args);
    }

    private void markAsOutsideCluster() throws IOException {
        clearDelegate();
        throw new NotCurrentLeaderException(
                String.format(
                        "This node isn't part of the coordination cluster for %s, and is thus not the leader.",
                        client));
    }

    private boolean isAssigned() {
        return coordinationService.getAssignment().getHostsForClient(client).contains(localhost);
    }

    private void clearDelegate() throws IOException {
        Object delegate = delegateReference.getAndSet(null);
        if (delegate instanceof Closeable) {
            ((Closeable) delegate).close();
        }
    }
}
