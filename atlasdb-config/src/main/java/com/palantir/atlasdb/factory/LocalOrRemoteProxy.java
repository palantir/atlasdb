/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.atlasdb.factory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.leader.NotCurrentLeaderException;

/**
 * A proxy that chooses between using a local or remote proxy based on a task that may take a while to complete.
 */
final class LocalOrRemoteProxy<T> extends AbstractInvocationHandler {

    static <U> U newProxyInstance(
            Class<U> interfaceClass,
            U localService,
            U remoteService,
            Future<Boolean> useLocalServiceFuture) {
        @SuppressWarnings("unchecked")
        U proxy = (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new LocalOrRemoteProxy<>(localService, remoteService, useLocalServiceFuture));
        return proxy;
    }

    private T localService;
    private T remoteService;
    private Future<Boolean> useLocalServiceFuture;

    private LocalOrRemoteProxy(T localService, T remoteService, Future<Boolean> useLocalServiceFuture) {
        this.localService = localService;
        this.remoteService = remoteService;
        this.useLocalServiceFuture = useLocalServiceFuture;
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        while (true) {
            try {
                return method.invoke(delegate(), args);
            } catch (InvocationTargetException e) {
                Throwable targetException = e.getTargetException();
                if (targetException instanceof NotCurrentLeaderException) {
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                    continue;
                }
                throw targetException;
            }
        }
    }

    private T delegate() {
        try {
            if (useLocalServiceFuture.get(0, TimeUnit.SECONDS)) {
                return localService;
            } else {
                return remoteService;
            }
        } catch (TimeoutException e) {
            // Use the remote service while we're still determining if it's safe to use the local service.
            return remoteService;
        } catch (InterruptedException e) {
            // Use the remote service while we're still determining if it's safe to use the local service.
            Thread.currentThread().interrupt();
            return remoteService;
        } catch (ExecutionException e) {
            // Using the remote service is always safe; fall back to it if anything went wrong
            return remoteService;
        }
    }
}
