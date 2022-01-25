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

import com.palantir.common.concurrent.ThreadNames;

/**
 * Whenever this runnable is run, for the duration of the call we will have a new thread name.
 * <p>
 * This is useful for debugging because when you get thread dumps the name will reflect what runnable
 * is running.
 *
 * Additionally, throwables emitted from the call are rewrapped to provide this custom annotation in the stack trace,
 * which should hopefully provide more context to what this runnable was attempting to do.
 */
public final class AnnotatedRunnable implements Runnable {

    private final Runnable delegate;
    private final String name;
    private final AnnotationType type;

    public static Runnable replaceThreadNameWith(String threadName, Runnable delegate) {
        return wrapWithThreadName(AnnotationType.REPLACE, threadName, delegate);
    }

    public static Runnable wrapWithThreadName(AnnotationType type, String threadName, Runnable delegate) {
        return new AnnotatedRunnable(delegate, threadName, type);
    }

    private AnnotatedRunnable(Runnable delegate, String name, AnnotationType type) {
        this.delegate = delegate;
        this.name = name;
        this.type = type;
    }

    @Override
    public void run() {
        final String oldName = Thread.currentThread().getName();
        ThreadNames.setThreadName(Thread.currentThread(), type.join(name, oldName));
        try {
            delegate.run();
        } catch (Throwable throwable) {
            throwable.addSuppressed(SuppressedException.from(throwable));
            throw throwable;
        } finally {
            ThreadNames.setThreadName(Thread.currentThread(), oldName);
        }
    }
}
