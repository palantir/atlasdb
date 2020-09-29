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
package com.palantir.nexus.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Iterables;
import com.palantir.common.proxy.TimingProxy;
import com.palantir.util.timer.LoggingOperationTimer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadConfinedProxyTest {

    Logger log = LoggerFactory.getLogger(ThreadConfinedProxyTest.class);

    String testString = "test";

    @Test
    public void testCurrentThreadCanCreateAndUseSubject() {
        @SuppressWarnings("unchecked")
        List<String> subject = ThreadConfinedProxy.newProxyInstance(List.class, new ArrayList<String>(),
                ThreadConfinedProxy.Strictness.VALIDATE);
        subject.add(testString);
        assertEquals(testString, Iterables.getOnlyElement(subject));
    }

    @Test
    public void testExplicitThreadCanCreateAndUseSubject() throws InterruptedException {

        final AtomicReference<List<String>> inputReference = new AtomicReference<List<String>>(null);
        final AtomicBoolean outputReference = new AtomicBoolean(false);

        Thread childThread = new Thread(() -> {
            List<String> subjectInChildThread = inputReference.get();
            subjectInChildThread.add(testString);
            if (Iterables.getOnlyElement(subjectInChildThread).equals(testString)) {
                outputReference.set(Boolean.TRUE);
            }
        });

        @SuppressWarnings("unchecked")
        List<String> subject = ThreadConfinedProxy.newProxyInstance(List.class, new ArrayList<String>(),
                ThreadConfinedProxy.Strictness.VALIDATE, childThread);
        inputReference.set(subject);
        childThread.start();
        childThread.join(10000);

        assertTrue(outputReference.get());
    }

    @Test
    public void testExplicitThreadCannotAndUseSubjectFromMainThread() throws InterruptedException {

        final AtomicReference<List<String>> inputReference = new AtomicReference<List<String>>(null);
        final AtomicBoolean outputReference = new AtomicBoolean(false);

        Thread childThread = new Thread(() -> {
            outputReference.set(true);
            List<String> subjectInChildThread = inputReference.get();
            subjectInChildThread.add(testString);
            // Should fail
            outputReference.set(false);
        });

        @SuppressWarnings("unchecked")
        List<String> subject = ThreadConfinedProxy.newProxyInstance(List.class, new ArrayList<String>(),
                ThreadConfinedProxy.Strictness.VALIDATE);
        inputReference.set(subject);
        childThread.start();
        childThread.join(10000);

        assertTrue(outputReference.get());
    }


    @Test
    public void testMainThreadCanDelegateToExplicitThreadAndLoseAccessAndAbilityToDelegate() throws InterruptedException {

        final AtomicReference<List<String>> inputReference = new AtomicReference<List<String>>(null);
        final AtomicInteger outputReference = new AtomicInteger(0);

        final Thread mainThread = Thread.currentThread();

        Thread childThread = new Thread(() -> {
            outputReference.compareAndSet(0, 1);
            List<String> subjectInChildThread = inputReference.get();
            ThreadConfinedProxy.changeThread(subjectInChildThread, mainThread, Thread.currentThread());
            subjectInChildThread.add(testString);
            if (Iterables.getOnlyElement(subjectInChildThread).equals(testString)) {
                outputReference.compareAndSet(1, 2);
            }
        });

        @SuppressWarnings("unchecked")
        List<String> subject = ThreadConfinedProxy.newProxyInstance(List.class, new ArrayList<String>(),
                ThreadConfinedProxy.Strictness.VALIDATE);
        inputReference.set(subject);
        childThread.start();
        childThread.join(10000);

        assertEquals(2, outputReference.get());

        // Cannot be access from main thread anymore
        try {
            subject.add(testString);
            fail();
        } catch (Exception e) {
            outputReference.compareAndSet(2, 3);
        }

        assertEquals(3, outputReference.get());

        // Cannot give to another thread because main thread does not own it
        Thread otherThread = new Thread(() -> {
            outputReference.compareAndSet(3, 4);
            List<String> subjectInChildThread = inputReference.get();
            ThreadConfinedProxy.changeThread(subjectInChildThread, mainThread, Thread.currentThread());
            outputReference.compareAndSet(4, 5);
        });

        otherThread.start();
        otherThread.join(10000);
        assertEquals(4, outputReference.get());

        // Cannot give away from main thread either
        try {
            ThreadConfinedProxy.changeThread(subject, mainThread, otherThread);
            fail();
        } catch (Exception e) {
            outputReference.compareAndSet(4, 5);
        }
        assertEquals(5, outputReference.get());
    }



    @Test
    public void testChildThreadCanDelegateBackToMainThread() throws InterruptedException {

        final AtomicReference<List<String>> inputReference = new AtomicReference<List<String>>(null);
        final AtomicInteger outputReference = new AtomicInteger(0);

        final Thread mainThread = Thread.currentThread();

        Thread childThread = new Thread(() -> {
            outputReference.compareAndSet(0, 1);
            List<String> subjectInChildThread = inputReference.get();
            ThreadConfinedProxy.changeThread(subjectInChildThread, mainThread, Thread.currentThread());
            subjectInChildThread.add(testString);
            if (Iterables.getOnlyElement(subjectInChildThread).equals(testString)) {
                outputReference.compareAndSet(1, 2);
            }
            ThreadConfinedProxy.changeThread(subjectInChildThread, Thread.currentThread(), mainThread);
        });

        @SuppressWarnings("unchecked")
        List<String> subject = ThreadConfinedProxy.newProxyInstance(List.class, new ArrayList<String>(),
                ThreadConfinedProxy.Strictness.VALIDATE);
        inputReference.set(subject);
        childThread.start();
        childThread.join(10000);

        assertEquals(2, outputReference.get());

        // We got delegated back, so we can use subject again
        assertEquals(testString, Iterables.getOnlyElement(subject));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testDelegationCanHandleMoreProxies() throws InterruptedException {

        final AtomicReference<List<String>> inputReference = new AtomicReference<List<String>>(null);
        final AtomicInteger outputReference = new AtomicInteger(0);

        final Thread mainThread = Thread.currentThread();

        Thread childThread = new Thread(() -> {
            outputReference.compareAndSet(0, 1);
            List<String> subjectInChildThread = inputReference.get();
            ThreadConfinedProxy.changeThread(subjectInChildThread, mainThread, Thread.currentThread());
            subjectInChildThread.add(testString);
            if (Iterables.getOnlyElement(subjectInChildThread).equals(testString)) {
                outputReference.compareAndSet(1, 2);
            }
            ThreadConfinedProxy.changeThread(subjectInChildThread, Thread.currentThread(), mainThread);
        });

        // Make sure subject is wrapped in proxies, including multiple ThreadConfinedProxy objects, and also does not start with a
        // ThreadConfinedProxy
        List<String> subject = new ArrayList<String>();
        subject = ThreadConfinedProxy.newProxyInstance(List.class, subject,
                ThreadConfinedProxy.Strictness.VALIDATE);
        subject = TimingProxy.newProxyInstance(List.class, subject, LoggingOperationTimer.create(log));
        subject = ThreadConfinedProxy.newProxyInstance(List.class, subject,
                ThreadConfinedProxy.Strictness.VALIDATE);
        subject = new DelegatingArrayListString(subject);
        subject = TimingProxy.newProxyInstance(List.class, subject, LoggingOperationTimer.create(log));

        inputReference.set(subject);
        childThread.start();
        childThread.join(10000);

        assertEquals(2, outputReference.get());

        // We got delegated back, so we can use subject again
        assertEquals(testString, Iterables.getOnlyElement(subject));
    }

    @Test
    public void testPropagateExceptions() throws SQLException {

        IThingThatThrows thing = ThreadConfinedProxy.newProxyInstance(IThingThatThrows.class, new ThingThatThrows(),
                ThreadConfinedProxy.Strictness.VALIDATE);

        assertEquals(1, thing.doStuff(IThingThatThrows.Behavior.RETURN_ONE));

        try {
            thing.doStuff(IThingThatThrows.Behavior.THROW_RUNTIME);
            fail("Should throw Runtime Exception");
        } catch (RuntimeException e) {
            // OK
        }

        try {
            thing.doStuff(IThingThatThrows.Behavior.THROW_SQL);
            fail("Should throw SQL Exception");
        } catch (SQLException e) {
            // OK
        }


    }

    private interface IThingThatThrows {

        enum Behavior {RETURN_ONE, THROW_RUNTIME, THROW_SQL;}

        int doStuff(Behavior b) throws SQLException;

    }

    private class ThingThatThrows implements IThingThatThrows {
        @Override
        public int doStuff(Behavior b) throws SQLException {
            switch (b) {
                case RETURN_ONE:
                    return 1;
                case THROW_RUNTIME:
                    throw new RuntimeException("Runtime");
                case THROW_SQL:
                    throw new SQLException("SQL");
                default:
                    return 0;
            }
        }
    }

    private class DelegatingArrayListString implements List<String>, Delegator<List<String>> {
        private final List<String> inner;
        public DelegatingArrayListString(List<String> subject) {
            inner = subject;
        }

        @Override
        public List<String> getDelegate() {
            return inner;
        }

        @Override
        public int size() {
            return inner.size();
        }

        @Override
        public boolean isEmpty() {
            return inner.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return inner.contains(o);
        }

        @Override
        public Iterator<String> iterator() {
            return inner.iterator();
        }

        @Override
        public Object[] toArray() {
            return inner.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return inner.toArray(a);
        }

        @Override
        public boolean add(String s) {
            return inner.add(s);
        }

        @Override
        public boolean remove(Object o) {
            return inner.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return inner.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends String> c) {
            return inner.addAll(c);
        }

        @Override
        public boolean addAll(int index, Collection<? extends String> c) {
            return inner.addAll(index, c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return inner.removeAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return inner.retainAll(c);
        }

        @Override
        public void clear() {
            inner.clear();
        }

        @Override
        public boolean equals(Object o) {
            return inner.equals(o);
        }

        @Override
        public int hashCode() {
            return inner.hashCode();
        }

        @Override
        public String get(int index) {
            return inner.get(index);
        }

        @Override
        public String set(int index, String element) {
            return inner.set(index, element);
        }

        @Override
        public void add(int index, String element) {
            inner.add(index, element);
        }

        @Override
        public String remove(int index) {
            return inner.remove(index);
        }

        @Override
        public int indexOf(Object o) {
            return inner.indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            return inner.lastIndexOf(o);
        }

        @Override
        public ListIterator<String> listIterator() {
            return inner.listIterator();
        }

        @Override
        public ListIterator<String> listIterator(int index) {
            return inner.listIterator(index);
        }

        @Override
        public List<String> subList(int fromIndex, int toIndex) {
            return inner.subList(fromIndex, toIndex);
        }

    }
}
