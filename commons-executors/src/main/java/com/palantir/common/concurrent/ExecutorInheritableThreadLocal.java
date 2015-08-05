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

/**
 * This class is like {@link InheritableThreadLocal} but it works with tasks spawned from the static
 * methods in {@link PTExecutors}. We don't usually use plain old threads for task management, so
 * {code InheritableThreadLocal} is not that useful. Use this class as a replacement.
 */
public class ExecutorInheritableThreadLocal<T> {

    private final static ThreadLocal<WeakHashMap<ExecutorInheritableThreadLocal<?>, Object>> mapForThisThread
            = new ThreadLocal<WeakHashMap<ExecutorInheritableThreadLocal<?>,Object>>() {
        @Override
        protected WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> initialValue() {
            return new WeakHashMap<ExecutorInheritableThreadLocal<?>, Object>();
        }
    };

    public void set(T value) {
        mapForThisThread.get().put(this, value);
    }

    public void remove() {
        Map<ExecutorInheritableThreadLocal<?>, Object> map = mapForThisThread.get();
        map.remove(this);
        if (map.isEmpty()) {
            mapForThisThread.remove();
        }
    }

    public T get() {
        if (mapForThisThread.get().containsKey(this)) {
            @SuppressWarnings("unchecked")
            T ret = (T) mapForThisThread.get().get(this);
            return ret;
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

    static WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> getMapForNewThread() {
        WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> currentMap = mapForThisThread.get();
        if (currentMap.isEmpty()) {
            mapForThisThread.remove();
            return currentMap;
        }
        WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> ret = new WeakHashMap<ExecutorInheritableThreadLocal<?>, Object>(currentMap.size());
        for (Map.Entry<ExecutorInheritableThreadLocal<?>, Object> e : currentMap.entrySet()) {
            ret.put(e.getKey(), e.getKey().callChildValue(e.getValue()));
        }

        return ret;
    }

    /**
     * @return the old map installed on that thread
     */
    static WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> installMapOnThread(WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> map) {
        WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> oldMap = mapForThisThread.get();
        if (map.isEmpty()) {
            mapForThisThread.remove();
        } else {
            // Temporarily install the untransformed map in case callInstallOnChildThread makes use
            // of existing thread locals (UserSessionClientInfo does this).
            mapForThisThread.set(map);
            WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> newMap = new WeakHashMap<ExecutorInheritableThreadLocal<?>, Object>(map);
            // Iterate over the new map so that 1) We modify entries in the new
            // map, not the old, and 2) so we don't get CMEs if
            // callInstallOnChildThread adds or removes any thread locals.
            for (Map.Entry<ExecutorInheritableThreadLocal<?>, Object> e : newMap.entrySet()) {
                e.setValue(e.getKey().callInstallOnChildThread(e.getValue()));
            }
            mapForThisThread.set(newMap);
        }
        return oldMap;
    }

    static void uninstallMapOnThread(WeakHashMap<ExecutorInheritableThreadLocal<?>, Object> oldMap) {
        try {
            for (ExecutorInheritableThreadLocal<?> eitl : mapForThisThread.get().keySet()) {
                eitl.uninstallOnChildThread();
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
    private T callChildValue(Object o) {
        return childValue((T) o);
    }

    @SuppressWarnings("unchecked")
    private T callInstallOnChildThread(Object o) {
        return installOnChildThread((T) o);
    }
}
