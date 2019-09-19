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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AnnotatedRunnableTest {

    private static final String THREAD_NAME = AnnotatedRunnableTest.class.getSimpleName();

    private String previousThreadName;

    @Before
    public void before() throws Exception {
        previousThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(THREAD_NAME);
    }

    @After
    public void checkThreadName() throws Exception {
        assertThat(Thread.currentThread().getName(), is(THREAD_NAME));
        Thread.currentThread().setName(previousThreadName);
    }

    @Test
    public void prependThreadName() throws Exception {
        AtomicBoolean hasRun = new AtomicBoolean(false);
        Runnable runnable = AnnotatedRunnable.wrapWithThreadName(
                AnnotationType.PREPEND, "test thread name", () -> {
                    assertThat(Thread.currentThread().getName(),
                            is("test thread name AnnotatedRunnableTest"));
                    hasRun.set(true);
                });

        runnable.run();
        assertThat(hasRun.get(), is(true));
    }

    @Test
    public void appendThreadName() throws Exception {
        AtomicBoolean hasRun = new AtomicBoolean(false);
        Runnable runnable = AnnotatedRunnable.wrapWithThreadName(
                AnnotationType.APPEND, "test thread name", () -> {
                    assertThat(Thread.currentThread().getName(),
                            is("AnnotatedRunnableTest test thread name"));
                    hasRun.set(true);
                });

        runnable.run();
        assertThat(hasRun.get(), is(true));
    }

    @Test
    public void replaceThreadName() throws Exception {
        AtomicBoolean hasRun = new AtomicBoolean(false);
        Runnable runnable = AnnotatedRunnable.wrapWithThreadName(
                AnnotationType.REPLACE, "test thread name", () -> {
                    assertThat(Thread.currentThread().getName(), is("test thread name"));
                    hasRun.set(true);
                });

        runnable.run();
        assertThat(hasRun.get(), is(true));
    }

    @Test
    public void replaceThreadNameWith() throws Exception {
        AtomicBoolean hasRun = new AtomicBoolean(false);
        Runnable runnable = AnnotatedRunnable.replaceThreadNameWith("test thread name", () -> {
            assertThat(Thread.currentThread().getName(), is("test thread name"));
            hasRun.set(true);
        });

        runnable.run();
        assertThat(hasRun.get(), is(true));
    }

    @Test
    public void annotateError() {
        AtomicBoolean hasRun = new AtomicBoolean(false);
        Runnable runnable = AnnotatedRunnable.wrapWithThreadName(
                AnnotationType.PREPEND, "test thread name", () -> {
                    assertThat(Thread.currentThread().getName(),
                            is("test thread name AnnotatedRunnableTest"));
                    throw new OutOfMemoryError("test message");
                });

        try {
            runnable.run();
            assertThat(hasRun, is(false));
            fail("Expected OutOfMemoryError");
        } catch (Throwable expected) {
            assertThat(expected, instanceOf(OutOfMemoryError.class));
            assertThat(expected.getMessage(), is("test message"));
            assertThat(expected.getSuppressed().length, is(1));
            assertThat(expected.getSuppressed()[0], instanceOf(SuppressedException.class));
            assertThat(expected.getSuppressed()[0].getMessage(),
                    is("Error [java.lang.OutOfMemoryError: test message]"
                            + " occurred while processing thread (test thread name AnnotatedRunnableTest)"));
        }
    }
}
