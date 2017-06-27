/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.nexus.db;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.common.proxy.DelegatingInvocationHandler;
import com.palantir.util.AssertUtils;

/**
 *  Dynamic Proxy for confining an object to a particular thread, but allowing explicit handoff.
 *
 *  For example, {@linkplain java.sql.Connection} objects are not thread-safe and are often passed around, sometimes between threads.  This
 *  can lead to race conditions.  Wrapping a Connection in a ThreadConfinedProxy will enforce that we do not accidentally access the
 *  Connection from multiple threads, provided we never expose the Connection outside of the proxy.
 */
public class ThreadConfinedProxy extends AbstractInvocationHandler implements DelegatingInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(ThreadConfinedProxy.class);

    public enum Strictness {
        ASSERT_AND_LOG, VALIDATE
    }

    private final Object delegate;
    private final Strictness strictness;

    @GuardedBy("this")
    private String threadName;
    @GuardedBy("this")
    private long threadId;


    private ThreadConfinedProxy(Object delegate, Strictness strictness, String threadName, long threadId) {
        this.delegate = delegate;
        this.strictness = strictness;
        this.threadName = threadName;
        this.threadId = threadId;
    }

    /**
     * Creates a new ThreadConfinedProxy with the given Strictness (ASSERT_AND_LOG or VALIDATE), initially assigned to the current thread.
     */
    public static <T> T newProxyInstance(Class<T> interfaceClass, T delegate, Strictness strictness) {
        return newProxyInstance(interfaceClass, delegate, strictness, Thread.currentThread());
    }

    /**
     * Explicitly passes the given ThreadConfinedProxy to a new thread.  If the proxy passed in is not a ThreadConfinedProxy, but is a
     * different type of proxy that also uses a {@linkplain DelegatingInvocationHandler}, this method will recursively apply to the
     * delegate.  This means that this method can handle arbitrarily nested DelegatingInvocationHandlers, including nested
     * ThreadConfinedProxy objects.
     *
     */
    public static void changeThread(Object proxy, Thread oldThread, Thread newThread) {
        Validate.notNull(proxy, "Proxy argument must not be null");
        if (Proxy.isProxyClass(proxy.getClass())) {
            InvocationHandler handler = Proxy.getInvocationHandler(proxy);
            changeHandlerThread(handler, oldThread, newThread);
        } else if (proxy instanceof Delegator) {
            changeThread(((Delegator) proxy).getDelegate(), oldThread, newThread);
        }
    }

    /**
     * Wraps a callable in a new callable that assigns ownership to the thread running the callable, then passes ownership back to
     * the thread that called this method.
     */
    public static <T> Callable<T> threadLendingCallable(final Object proxy, final Callable<T> callable) {
        if (proxy == null) {
            return callable;
        }
        final Thread parent = Thread.currentThread();
        return new Callable<T>() {
            @Override
            public T call() throws Exception {
                Thread child = Thread.currentThread();
                changeThread(proxy, parent, child);
                try {
                    return callable.call();
                } finally {
                    changeThread(proxy, child, parent);
                }
            }
        };
    }

    private static void changeHandlerThread(InvocationHandler handler, Thread oldThread, Thread newThread) {
        if (handler instanceof ThreadConfinedProxy) {
            ((ThreadConfinedProxy) handler).changeThread(oldThread, newThread);
        }
        if (handler instanceof DelegatingInvocationHandler) {
            changeThread(((DelegatingInvocationHandler) handler).getDelegate(), oldThread, newThread);
        }
    }

    /**
     * Creates a new ThreadConfinedProxy with the given Strictness (ASSERT_AND_LOG or VALIDATE), initially assigned to the given thread.
     */
    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(Class<T> interfaceClass, T delegate, Strictness strictness, Thread current) {
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(),
                new Class<?>[] {interfaceClass}, new ThreadConfinedProxy(delegate, strictness, current.getName(), current.getId()));
    }


    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        checkThread(method);
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    /*
     * Enforce we were called from the correct thread.
     * Note that thread names are not stable, so we must use thread IDs.
     * Names are for help debugging.
     *
     * Synchronized for threadId and threadName.
     */
    private synchronized void checkThread(Method method) {
        Thread current = Thread.currentThread();
        if (threadId != current.getId()) {
            String message = String.format(
                    "Thread confinement violation: method %s#%s was called from thread %s (ID %s) instead of thread %s (ID %s)",
                    method.getDeclaringClass().getCanonicalName(),
                    method.getName(),
                    current.getName(),
                    current.getId(),
                    threadName,
                    threadId);

            fail(message);
        }
    }

    private void fail(String message) {
        switch (strictness) {
            case ASSERT_AND_LOG:
                AssertUtils.assertAndLog(log, false, message);
                break;
            case VALIDATE:
                Validate.isTrue(false, message);
                break;
        }
    }

    private synchronized void changeThread(Thread oldThread, Thread newThread) {
        checkThreadChange(oldThread, newThread);
        threadId = newThread.getId();
        threadName = newThread.getName();
    }

    private synchronized void checkThreadChange(Thread oldThread, Thread newThread) {
        if (oldThread.getId() != threadId) {
            String message = String.format(
                    "Thread confinement violation: tried to change threads from thread %s (ID %s) to thread %s (ID %s), but we expected thread %s (ID %s)",
                    oldThread.getId(),
                    oldThread.getName(),
                    newThread.getId(),
                    newThread.getName(),
                    threadName,
                    threadId);

            fail(message);

        }
    }

    @Override
    public Object getDelegate() {
        return delegate;
    }
}
