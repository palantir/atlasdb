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

import java.util.concurrent.Callable;

import com.google.common.base.Supplier;

/**
 * This is a context that is injected into services.
 * We need a layer of indirection because these Services will be long lived, but we don't know the value of a
 * context until the service is going to be called.  An example of something that would be injected with a
 * ServiceContext is a transaction object that the service needed to do its job.
 * <p>
 * The context should be set before the service that consumes it is invoked.  The best way to ensure this is to wrap
 * it in a {@link PopulateServiceContextProxy}
 *
 * @see SupplierProxy for services that aren't set up to take a ServiceContext
 */
public interface ServiceContext<T> extends Supplier<T> {
    /**
     * This runs the passed callable with the provided context.
     * <p>
     * All implementers of this method must set the context in a try finally block.  The best way to do this is to
     * extend {@link AbstractWritableServiceContext}
     * <p>
     * Some Contexts are not writable and will throw an {@link UnsupportedOperationException} if you try to call them
     * with a context.  In general context should be set by proxies to inject the state when needed.
     * <p>
     * <pre>
        T oldValue = get();
        set(context);
        try {
            return callable.call();
        } finally {
            set(oldValue);
        }
     * </pre>
     * @see AbstractWritableServiceContext
     */
    public <R> R callWithContext(T context, Callable<R> callable) throws Exception;
}
