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
package com.palantir.atlasdb.util;

import java.util.concurrent.Callable;

/**
 * Whenever this callable is called, for the duration of the call we will have a new thread name.
 * <p>
 * This is useful for debugging because when you get thread dumps the name will reflect what callable
 * is running.
 *
 * Additionally, throwables emitted from the call are rewrapped to provide this custom annotation in the stack trace,
 * which should hopefully provide more context to what this callable was attempting to do.
 */
public final class AnnotatedCallable<T> implements Callable<T> {

    private final Callable<T> delegate;
    private final String name;
    private final AnnotationType type;

    public static <T> Callable<T> replaceThreadNameWith(String threadName, Callable<T> delegate) {
        return wrapWithThreadName(AnnotationType.REPLACE, threadName, delegate);
    }

    public static <T> Callable<T> wrapWithThreadName(AnnotationType _type, String _threadName, Callable<T> delegate) {
        return delegate;
    }

    private AnnotatedCallable(Callable<T> delegate, String name, AnnotationType type) {
        this.delegate = delegate;
        this.name = name;
        this.type = type;
    }

    @Override
    public T call() throws Exception {
        final String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName(type.join(name, oldName));
        try {
            return delegate.call();
        } catch (Throwable throwable) {
            throwable.addSuppressed(SuppressedException.from(throwable));
            throw throwable;
        } finally {
            Thread.currentThread().setName(oldName);
        }
    }
}
