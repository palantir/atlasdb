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
package com.palantir.common.concurrent;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class is like {@link InheritableThreadLocal} but it works with tasks spawned from the static
 * methods in <code>PTExecutors</code>. We don't usually use plain old threads for task management, so
 * {code InheritableThreadLocal} is not that useful. Use this class as a replacement.
 */
public class ExecutorInheritableThreadLocal<T> {

    private static final ThreadLocal<ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object>>
            mapForThisThread = ThreadLocal.withInitial(ExecutorInheritableThreadLocal::makeNewMap);

    private final WeakReference<ExecutorInheritableThreadLocal<T>> myReference;

    public ExecutorInheritableThreadLocal() {
        myReference = new WeakReference<>(this);
    }

    private enum NullWrapper {
        INSTANCE
    }

    public void set(T value) {
        mapForThisThread.get().put(myReference, wrapNull(value));
    }

    public void remove() {
        Map<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> map = mapForThisThread.get();
        map.remove(myReference);
        if (map.isEmpty()) {
            mapForThisThread.remove();
        }
    }

    public T get() {
        ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> myThreadLocals =
                mapForThisThread.get();
        Object value = myThreadLocals.get(myReference);
        if (value != null) {
            return unwrapNull(value);
        } else {
            T ret = initialValue();
            set(ret);
            return ret;
        }
    }

    /**
     * Computes the child's initial value for this thread-local variable as a function of the
     * parent's value. This method is called from within the parent thread during the submit call to
     * the Executor. The value returned will be that of the child's when it is run by the executor.
     * <p>
     * This method merely returns its input argument, and should be overridden if a different
     * behavior is desired.
     *
     * @param parentValue the parent thread's value
     * @return the child thread's initial value
     */
    protected T childValue(T parentValue) {
        return parentValue;
    }

    /**
     * Computes the child's initial value for this thread-local variable as a function of the result
     * of {@link #childValue(Object)} (which is called on the parent thread). This method is run on
     * the child thread.
     * <p>
     * This method merely returns its input argument, and should be overridden if a different
     * behavior is desired.
     *
     * @param childValue the return value from {@link #childValue(Object)}
     * @return the initial value that will be used for this thread
     */
    protected T installOnChildThread(T childValue) {
        return childValue;
    }

    /**
     * Called by the executor service when the submitted task is complete. This method can be used
     * to perform cleanup activities on the child thread.
     * <p>
     * By default this method is a no-op, and should be overridden if different behavior is desired.
     * <p>
     * This will be run from a finally block, so it should not throw.
     * <p>
     * NOTE: This code isn't guaranteed to finish by the time future.get() returns in the calling thread.
     * The completed Future is marked <code>isDone</code> and then this is run in a finally block.
     */
    protected void uninstallOnChildThread() {
        /* Do nothing. */
    }

    /**
     * Returns the current thread's initial value for this thread-local variable. This method will
     * be invoked at most once per accessing thread for each thread-local, the first time the thread
     * accesses the variable with the {@link #get} method. The <tt>initialValue</tt> method will not
     * be invoked in a thread if the thread invokes the {@link #set} method prior to the
     * <tt>get</tt> method.
     * <p>
     * This implementation simply returns <tt>null</tt>; if the programmer desires thread-local
     * variables to be initialized to some value other than <tt>null</tt>, <tt>ThreadLocal</tt> must
     * be subclassed, and this method overridden. Typically, an anonymous inner class will be used.
     * Typical implementations of <tt>initialValue</tt> will invoke an appropriate constructor and
     * return the newly constructed object.
     *
     * @return the initial value for this thread-local
     */
    protected T initialValue() {
        return null;
    }

    // This project is used by api projects outside of atlasdb that try very hard to minimize
    // their dependency footprint. As such we don't want to force users to take guava as a dependency.
    // Widely used legacy code, so not retroactively changing mixed mutability return types.
    @SuppressWarnings({"AvoidNewHashMapInt", "MixedMutabilityReturnType"})
    static Map<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> getMapForNewThread() {
        ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> currentMap =
                mapForThisThread.get();
        if (currentMap.isEmpty()) {
            mapForThisThread.remove();
            return Collections.emptyMap();
        }
        Map<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> ret =
                new HashMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object>(currentMap.size());
        for (WeakReference<? extends ExecutorInheritableThreadLocal<?>> ref : currentMap.keySet()) {
            @SuppressWarnings("unchecked")
            ExecutorInheritableThreadLocal<Object> threadLocal = (ExecutorInheritableThreadLocal<Object>) ref.get();
            if (threadLocal == null) {
                currentMap.remove(ref);
            } else {
                ret.put(ref, wrapNull(threadLocal.callChildValue(threadLocal.get())));
            }
        }
        return ret;
    }

    /**
     * Installs a map on the given thread.
     *
     * @return the old map installed on that thread
     */
    static ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> installMapOnThread(
            Map<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> map) {
        ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> oldMap =
                mapForThisThread.get();
        if (map.isEmpty()) {
            mapForThisThread.remove();
        } else {
            ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> newMap = makeNewMap();
            newMap.putAll(map);

            // Install the map in case callInstallOnChildThread makes use
            // of existing thread locals (UserSessionClientInfo does this).
            mapForThisThread.set(newMap);

            for (WeakReference<? extends ExecutorInheritableThreadLocal<?>> ref : map.keySet()) {
                @SuppressWarnings("unchecked")
                ExecutorInheritableThreadLocal<Object> threadLocal = (ExecutorInheritableThreadLocal<Object>) ref.get();
                if (threadLocal != null) {
                    threadLocal.set(threadLocal.callInstallOnChildThread(threadLocal.get()));
                }
            }
        }
        return oldMap;
    }

    private static ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> makeNewMap() {
        return new ConcurrentHashMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object>();
    }

    private static Object wrapNull(Object value) {
        return value == null ? NullWrapper.INSTANCE : value;
    }

    @SuppressWarnings("unchecked")
    private T unwrapNull(Object ret) {
        if (ret == NullWrapper.INSTANCE) {
            return null;
        } else {
            return (T) ret;
        }
    }

    static void uninstallMapOnThread(
            ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> oldMap) {
        try {
            for (WeakReference<? extends ExecutorInheritableThreadLocal<?>> ref :
                    mapForThisThread.get().keySet()) {
                @SuppressWarnings("unchecked")
                ExecutorInheritableThreadLocal<Object> threadLocal = (ExecutorInheritableThreadLocal<Object>) ref.get();
                if (threadLocal != null) {
                    threadLocal.uninstallOnChildThread();
                }
            }
        } finally {
            if (oldMap.isEmpty()) {
                mapForThisThread.remove();
            } else {
                mapForThisThread.set(oldMap);
            }
        }
    }

    @Override
    public final boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    @SuppressWarnings("unchecked")
    private T callChildValue(Object obj) {
        return childValue((T) obj);
    }

    @SuppressWarnings("unchecked")
    private T callInstallOnChildThread(Object obj) {
        return installOnChildThread((T) obj);
    }
}
