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
package com.palantir.common.base;

import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.exception.PalantirInterruptedException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for creating and propagating exceptions.
 *
 * Note: Differently from Guava, the methods in this class handle interruption; that is, they will re-set the
 * interrupt flag and throw a {@link PalantirInterruptedException} instead of {@link PalantirRuntimeException}
 * if incoming exceptions are subclasses of {@link InterruptedException}.
 */
public final class Throwables {

    private static Logger log = LoggerFactory.getLogger(Throwables.class);

    private Throwables() {
        /* uninstantiable */
    }

    /**
     * Simply call throwable.initCause(cause) and return throwable.
     * This makes it easy to chain for old exception types with no chained constructor.
     * <p>
     * <code>
     * throw Throwables.chain(new MyExceptionTypeWithNoChainedConstructor(cause.getMessage()), cause)
     * </code>
     * <p>
     * instead of
     * <p>
     * <code>
     * MyExceptionTypeWithNoChainedConstructor throwThis = new MyExceptionTypeWithNoChainedConstructor(cause.getMessage);
     * <br>
     * throwThis.initCause(cause);
     * <br>
     * throw throwThis;
     * </code>
     */
    public static <T extends Throwable> T chain(T throwable, Throwable cause) {
        throwable.initCause(cause);
        return throwable;
    }

    /**
     * If Throwable is a RuntimeException or Error, rewrap and throw it. If not, throw a PalantirRuntimeException.
     */
    public static RuntimeException rewrapAndThrowUncheckedException(Throwable ex) {
        throw rewrapAndThrowUncheckedException(ex.getMessage(), ex);
    }

    /**
     * If Throwable is a RuntimeException or Error, rewrap and throw it. If not, throw a PalantirRuntimeException.
     */
    public static RuntimeException rewrapAndThrowUncheckedException(String newMessage, Throwable ex) {
        rewrapAndThrowIfInstance(newMessage, ex, RuntimeException.class);
        rewrapAndThrowIfInstance(newMessage, ex, Error.class);
        throw createPalantirRuntimeException(newMessage, ex);
    }

    /**
     * If Throwable is a RuntimeException or Error, rethrow it. If its an ExecutionException or
     * InvocationTargetException, extract the cause and process it. Else, throw a PalantirRuntimeException.
     */
    public static AtlasDbDependencyException unwrapAndThrowAtlasDbDependencyException(Throwable ex) {
        throw createAtlasDbDependencyException(unwrapIfPossible(ex));
    }

    public static Throwable unwrapIfPossible(Throwable ex) {
        if (ex instanceof ExecutionException || ex instanceof InvocationTargetException) {
            return ex.getCause();
        }
        return ex;
    }

    private static RuntimeException createAtlasDbDependencyException(Throwable ex) {
        if (ex instanceof InterruptedException || ex instanceof InterruptedIOException) {
            Thread.currentThread().interrupt();
        }
        throwIfInstance(ex, AtlasDbDependencyException.class);
        return new AtlasDbDependencyException(ex);
    }

    /**
     * Throws the input Throwable if it is a RuntimeException or Error, otherwise wraps it in a
     * PalantirRuntimeException.
     */
    public static RuntimeException throwUncheckedException(Throwable ex) {
        throwIfUncheckedException(ex);
        throw createPalantirRuntimeException(ex);
    }

    private static RuntimeException createPalantirRuntimeException(Throwable ex) {
        return createPalantirRuntimeException(ex.getMessage(), ex);
    }

    private static RuntimeException createPalantirRuntimeException(String newMessage, Throwable ex) {
        if (ex instanceof InterruptedException || ex instanceof InterruptedIOException) {
            Thread.currentThread().interrupt();
            return new PalantirInterruptedException(newMessage, ex);
        }
        return new PalantirRuntimeException(newMessage, ex);
    }

    /**
     * Will simply rethrow the exception if it is a {@link RuntimeException} or an {@link Error}
     */
    public static void throwIfUncheckedException(Throwable ex) {
        throwIfInstance(ex, RuntimeException.class);
        throwIfInstance(ex, Error.class);
    }

    /**
     * if (t instanceof K) throw Throwables.rewrap((K)t);
     * <p>
     * Note: The runtime type of the thrown exception will be the same as t even if
     * clazz is a supertype of t.
     */
    public static <K extends Throwable> void rewrapAndThrowIfInstance(Throwable t, Class<K> clazz) throws K {
        rewrapAndThrowIfInstance(t == null ? "null" : t.getMessage(), t, clazz);
    }

    /**
     * if (t instanceof K) throw Throwables.rewrap((K)t);
     * <p>
     * Note: The runtime type of the thrown exception will be the same as t even if
     * clazz is a supertype of t.
     */
    @SuppressWarnings("unchecked")
    public static <K extends Throwable> void rewrapAndThrowIfInstance(String newMessage, Throwable t, Class<K> clazz)
            throws K {
        if ((t != null) && clazz.isAssignableFrom(t.getClass())) {
            K kt = (K) t;
            K wrapped = Throwables.rewrap(newMessage, kt);
            throw wrapped;
        }
    }

    /**
     * if (t instanceof K) throw (K)t;
     * <p>
     * Note: The runtime type of the thrown exception will be the same as t even if
     * clazz is a supertype of t.
     */
    @SuppressWarnings("unchecked")
    public static <K extends Throwable> void throwIfInstance(Throwable t, Class<K> clazz) throws K {
        if (isInstance(t, clazz)) {
            K kt = (K) t;
            throw kt;
        }
    }

    private static <K extends Throwable> boolean isInstance(Throwable t, Class<K> clazz) {
        return (t != null) && clazz.isAssignableFrom(t.getClass());
    }

    /**
     * Rewraps the given throwable in a newly created throwable of the same runtime type in order to capture the current
     * thread's stack trace.  Use this method when you are about to rethrow a throwable from another thread,
     * for example when throwing {@link ExecutionException#getCause()} after calling {@link Future#get()};
     */
    public static <T extends Throwable> T rewrap(T throwable) {
        Preconditions.checkNotNull(throwable);
        return rewrap(throwable.getMessage(), throwable);
    }

    /**
     * Rewraps the given throwable in a newly created throwable of the same runtime type in order to capture the current
     * thread's stack trace.  Use this method when you are about to rethrow a throwable from another thread,
     * for example when throwing {@link ExecutionException#getCause()} after calling {@link Future#get()};
     */
    public static <T extends Throwable> T rewrap(final String newMessage, final T throwable) {
        Preconditions.checkNotNull(throwable);
        log.info("Rewrapping throwable {} with newMessage {}",
                UnsafeArg.of("wrappedThrowable", throwable),
                UnsafeArg.of("newMessage", newMessage))
        ;
        try {
            Constructor<?>[] constructors = throwable.getClass().getConstructors();
            // First see if we can create the exception in a way that lets us preserve the message text
            for (Constructor<?> c : constructors) {
                if (Arrays.equals(new Class<?>[] {String.class, Throwable.class}, c.getParameterTypes())) {
                    @SuppressWarnings("unchecked")
                    T rv = (T) c.newInstance(newMessage, throwable);
                    return rv;
                } else if (Arrays.equals(new Class<?>[] {String.class}, c.getParameterTypes())) {
                    @SuppressWarnings("unchecked")
                    T rv = (T) c.newInstance(newMessage);
                    return chain(rv, throwable);
                }
            }
            return throwable;

        } catch (Exception e) {
            // If something goes wrong when we try to rewrap the exception,
            // we should log and throw a runtime exception.
            log.error("Unexpected error encountered while rewrapping throwable of class {}", throwable.getClass(), e);
            throw createPalantirRuntimeException(newMessage, throwable);
        }
    }

    public static RuntimeException throwCauseAsUnchecked(Exception exception) {
        Throwable cause = exception.getCause();

        if (cause == null) {
            log.warn("Exceptions passed to throwCauseAsUnchecked should have a cause", cause);
            throw new SafeIllegalStateException("Exceptions passed to throwCauseAsUnchecked should have a cause");
        }
        throwIfUncheckedException(cause);
        throw new RuntimeException(cause);
    }

    /**
     * Returns a dump of all threads.
     * @return
     */
    public static String getThreadDump() {
        return printThreadDump(Thread.getAllStackTraces());
    }

    /**
     * This method prints a series of stack traces.  It is meant to be used with the output from
     * Threads.getAllStackTraces.
     * @param map
     * @return
     */
    private static String printThreadDump(Map<Thread, StackTraceElement[]> map) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        for (Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
            Thread t = entry.getKey();
            StackTraceElement elements[] = entry.getValue();

            printWriter.println(new StringBuilder().append(t));
            printStackTrace(printWriter, elements);
            printWriter.println();
        }

        return stringWriter.toString();
    }

    private static void printStackTrace(PrintWriter printwriter, StackTraceElement elements[]) {
        synchronized (printwriter) {
            for (int i = 0; i < elements.length; i++) {
                printwriter.println("\tat " + elements[i]);
            }
        }
    }
}
