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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AnnotatedCallableTest {

    private static final String THREAD_NAME = AnnotatedCallableTest.class.getSimpleName();

    private String previousThreadName;

    @Before
    public void before() throws Exception {
        previousThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(THREAD_NAME);
    }

    @After
    public void after() throws Exception {
        assertThat(Thread.currentThread().getName()).isEqualTo(THREAD_NAME);
        Thread.currentThread().setName(previousThreadName);
    }

    @Test
    public void prependThreadName() throws Exception {
        Callable<String> callable = AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND, "test", () -> {
            assertThat(Thread.currentThread().getName()).isEqualTo("test AnnotatedCallableTest");
            return "Hello, world!";
        });

        String result = callable.call();
        assertThat(result).isEqualTo("Hello, world!");
    }

    @Test
    public void appendThreadName() throws Exception {
        Callable<String> callable = AnnotatedCallable.wrapWithThreadName(AnnotationType.APPEND, "test", () -> {
            assertThat(Thread.currentThread().getName()).isEqualTo("AnnotatedCallableTest test");
            return "Hello, world!";
        });

        String result = callable.call();
        assertThat(result).isEqualTo("Hello, world!");
    }

    @Test
    public void replaceThreadName() throws Exception {
        Callable<String> callable = AnnotatedCallable.wrapWithThreadName(AnnotationType.REPLACE, "test", () -> {
            assertThat(Thread.currentThread().getName()).isEqualTo("test");
            return "Hello, world!";
        });

        String result = callable.call();
        assertThat(result).isEqualTo("Hello, world!");
    }

    @Test
    public void replaceThreadNameWith() throws Exception {
        Callable<String> callable = AnnotatedCallable.replaceThreadNameWith("test", () -> {
            assertThat(Thread.currentThread().getName()).isEqualTo("test");
            return "Hello, world!";
        });

        String result = callable.call();
        assertThat(result).isEqualTo("Hello, world!");
    }

    @Test
    public void annotateError() {
        Callable<String> callable =
                AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND, "test thread name", () -> {
                    assertThat(Thread.currentThread().getName()).startsWith("test thread name ");
                    assertThat(Thread.currentThread().getName()).isEqualTo("test thread name AnnotatedCallableTest");
                    throw new OutOfMemoryError("test message");
                });

        try {
            String result = callable.call();
            fail("Expected OutOfMemoryError");
        } catch (Throwable expected) {
            assertThat(expected).isInstanceOf(OutOfMemoryError.class);
            assertThat(expected.getMessage()).isEqualTo("test message");
            assertThat(expected.getSuppressed()).hasSize(1);
            assertThat(expected.getSuppressed()[0]).isInstanceOf(SuppressedException.class);
            assertThat(expected.getSuppressed()[0].getMessage())
                    .isEqualTo("Error [java.lang.OutOfMemoryError: test message]"
                            + " occurred while processing thread (test thread name AnnotatedCallableTest)");
        }
    }

    @Test
    public void annotateException() {
        Callable<String> callable =
                AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND, "test thread name", () -> {
                    assertThat(Thread.currentThread().getName()).startsWith("test thread name ");
                    assertThat(Thread.currentThread().getName()).isEqualTo("test thread name AnnotatedCallableTest");
                    throw new IOException("test message");
                });

        try {
            String result = callable.call();
            fail("Expected Exception");
        } catch (Throwable expected) {
            assertThat(expected).isInstanceOf(IOException.class);
            assertThat(expected.getMessage()).isEqualTo("test message");
            assertThat(expected.getSuppressed()).hasSize(1);
            assertThat(expected.getSuppressed()[0]).isInstanceOf(SuppressedException.class);
            assertThat(expected.getSuppressed()[0].getMessage())
                    .isEqualTo("Exception [java.io.IOException: test message]"
                            + " occurred while processing thread (test thread name AnnotatedCallableTest)");
        }
    }

    @Test
    public void annotateExecutionException() {
        final Throwable cause = new Throwable("test cause");
        Callable<String> callable =
                AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND, "test thread name", () -> {
                    assertThat(Thread.currentThread().getName()).startsWith("test thread name ");
                    assertThat(Thread.currentThread().getName()).isEqualTo("test thread name AnnotatedCallableTest");
                    throw new ExecutionException("test message", cause);
                });

        try {
            String result = callable.call();
            fail("Expected Exception");
        } catch (Throwable expected) {
            assertThat(expected).isInstanceOf(ExecutionException.class);
            assertThat(expected.getMessage()).isEqualTo("test message");
            assertThat(expected.getSuppressed()).hasSize(1);
            assertThat(expected.getSuppressed()[0]).isInstanceOf(SuppressedException.class);
            assertThat(expected.getSuppressed()[0].getMessage())
                    .isEqualTo("Exception [java.util.concurrent.ExecutionException: test message]"
                            + " occurred while processing thread (test thread name AnnotatedCallableTest)");
        }
    }

    @Test
    public void annotateRuntimeException() {
        Callable<String> callable =
                AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND, "test thread name", () -> {
                    assertThat(Thread.currentThread().getName()).startsWith("test thread name ");
                    assertThat(Thread.currentThread().getName()).isEqualTo("test thread name AnnotatedCallableTest");
                    throw new IllegalArgumentException("test message");
                });

        try {
            String result = callable.call();
            fail("Expected RuntimeException");
        } catch (Throwable expected) {
            assertThat(expected).isInstanceOf(RuntimeException.class);
            assertThat(expected.getMessage()).isEqualTo("test message");
            assertThat(expected.getSuppressed()).hasSize(1);
            assertThat(expected.getSuppressed()[0]).isInstanceOf(SuppressedException.class);
            assertThat(expected.getSuppressed()[0].getMessage())
                    .isEqualTo("RuntimeException [java.lang.IllegalArgumentException: test message]"
                            + " occurred while processing thread (test thread name AnnotatedCallableTest)");
        }
    }
}
