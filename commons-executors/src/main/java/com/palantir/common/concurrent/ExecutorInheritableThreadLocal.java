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
package com.palantir.common.concurrent;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * This class is like {@link InheritableThreadLocal} but it works with tasks spawned from the static
 * methods in {@link PTExecutors}. We don't usually use plain old threads for task management, so
 * {code InheritableThreadLocal} is not that useful. Use this class as a replacement.
 */
public class ExecutorInheritableThreadLocal<T> {

    private final static ThreadLocal<LoadingCache<ExecutorInheritableThreadLocal<?>, Object>> perThreadThreadCache = new ThreadLocal<LoadingCache<ExecutorInheritableThreadLocal<?>, Object>>() {
        @Override
        protected LoadingCache<ExecutorInheritableThreadLocal<?>, Object> initialValue() {
            return CacheBuilder.newBuilder().weakKeys().build(
                    new CacheLoader<ExecutorInheritableThreadLocal<?>, Object>() {
                        @Override
                        public Object load(ExecutorInheritableThreadLocal<?> key) throws Exception {
                            return new WeakHashMap<ExecutorInheritableThreadLocal<?>, Object>();
                        }
                    });
        }
    };

    private class NullWrapper {

    }

    public void set(T value) {
        if (value == null) {
            perThreadThreadCache.get().put(this, new NullWrapper());
        } else {
            perThreadThreadCache.get().put(this, value);
        }
    }

    public void remove() {
        LoadingCache<ExecutorInheritableThreadLocal<?>, Object> threadLocalCache = perThreadThreadCache.get();

        threadLocalCache.invalidate(this);
        if (threadLocalCache.size() == 0) {
            perThreadThreadCache.remove();
        }
    }

    public T get() {
        if (perThreadThreadCache.get().asMap().containsKey(this)) {
            T ret = (T) perThreadThreadCache.get().getUnchecked(this);
            if (NullWrapper.class.isInstance(ret)) {
                return null;
            } else {
                return ret;
            }
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

    static ConcurrentMap<ExecutorInheritableThreadLocal<?>, Object> getMapForNewThread() {
        ConcurrentMap<ExecutorInheritableThreadLocal<?>, Object> currentMap = perThreadThreadCache.get().asMap();
        if (currentMap.isEmpty()) {
            perThreadThreadCache.remove();
            return currentMap;
        }
        ConcurrentMap<ExecutorInheritableThreadLocal<?>, Object> ret = new ConcurrentHashMap<ExecutorInheritableThreadLocal<?>, Object>(currentMap.size());
        for (Map.Entry<ExecutorInheritableThreadLocal<?>, Object> e : currentMap.entrySet()) {
            ret.put(e.getKey(), e.getKey().callChildValue(e.getValue()));
        }

        return ret;
    }

    /**
     * @return the old map installed on that thread
     */
    static ConcurrentMap<ExecutorInheritableThreadLocal<?>, Object> installMapOnThread(ConcurrentMap<ExecutorInheritableThreadLocal<?>, Object> map) {
        ConcurrentMap<ExecutorInheritableThreadLocal<?>, Object> oldMap = perThreadThreadCache.get().asMap();
        if (map.isEmpty()) {
            perThreadThreadCache.remove();
        } else {
            // Temporarily install the untransformed map in case callInstallOnChildThread makes use
            // of existing thread locals (UserSessionClientInfo does this).
            perThreadThreadCache.get().asMap().putAll(map);
            WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> newMap = new WeakHashMap<ExecutorInheritableThreadLocal<?>, Object>(map);
            // Iterate over the new map so that 1) We modify entries in the new
            // map, not the old, and 2) so we don't get CMEs if
            // callInstallOnChildThread adds or removes any thread locals.
            for (Map.Entry<ExecutorInheritableThreadLocal<?>, Object> e : newMap.entrySet()) {
                e.setValue(e.getKey().callInstallOnChildThread(e.getValue()));
            }
            perThreadThreadCache.get().asMap().putAll(newMap);
        }
        return oldMap;
    }

    static void uninstallMapOnThread(ConcurrentMap<ExecutorInheritableThreadLocal<?>, Object> oldMap) {
        try {
            for (ExecutorInheritableThreadLocal<?> eitl : perThreadThreadCache.get().asMap().keySet()) {
                eitl.uninstallOnChildThread();
            }
        } finally {
            if (oldMap.isEmpty()) {
                perThreadThreadCache.remove();
            } else {
                perThreadThreadCache.get().asMap().putAll(oldMap);
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
    private T callChildValue(Object o) {
        return childValue((T) o);
    }

    @SuppressWarnings("unchecked")
    private T callInstallOnChildThread(Object o) {
        return installOnChildThread((T) o);
    }
}
