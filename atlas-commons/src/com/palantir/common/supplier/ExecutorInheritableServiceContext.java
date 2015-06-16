// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.common.supplier;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import com.palantir.common.concurrent.ExecutorInheritableThreadLocal;

/**
 * This class implements a ServiceContext that passes it's context to tasks submitted to an
 * {@link Executor}.
 * <p>
 * At {@link ExecutorService#submit(java.util.concurrent.Callable)} time, whatever the state of
 * this context is, the task executing in the thread will inherit that.
 *
 * @author carrino
 */
public class ExecutorInheritableServiceContext<T> extends AbstractWritableServiceContext<T> {
    final private ExecutorInheritableThreadLocal<T> delegate = new ExecutorInheritableThreadLocal<T>();
    private ExecutorInheritableServiceContext() { /* */ }

    public static <T> ExecutorInheritableServiceContext<T> create() {
        return new ExecutorInheritableServiceContext<T>();
    }

    @Override
    public T get() {
        T ret = delegate.get();
        if (ret == null) {
            delegate.remove();
        }
        return ret;
    }

    @Override
    protected void set(T newValue) {
        if (newValue == null) {
            delegate.remove();
        } else {
            delegate.set(newValue);
        }
    }
}
