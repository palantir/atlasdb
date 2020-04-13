/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.iterators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.atlasdb.v2.api.api.AsyncIterator;

public class AsyncIteratorsTest {
    private static final String THROW = "throw";
    private final AsyncIterators iterators = new AsyncIterators(MoreExecutors.directExecutor());
    private final Iterator<String> iterator = ImmutableSet.of("something", THROW).iterator();
    private final AsyncIterator<String> async = new NonBlockingIterator<>(iterator);

    @Test
    public void testForEachFailsIfElementFails() {
        RuntimeException toThrow = new RuntimeException();
        assertThatThrownBy(() -> Futures.getUnchecked(iterators.forEach(async, element -> {
            if (element.equals(THROW)) {
                throw toThrow;
            }
        }))).isInstanceOf(UncheckedExecutionException.class).hasCause(toThrow);
    }

    @Test
    public void testTakeWhile_termination() {
        TestIterator iterator = new TestIterator();
        List<Integer> trace = new ArrayList<>();
        ListenableFuture<?> done = iterators.takeWhile(iterator, element -> {
            trace.add(element);
            return element < 2;
        });
        iterator.setNextElement(0);
        assertThat(done).isNotDone();
        iterator.setNextElement(1);
        assertThat(done).isNotDone();
        iterator.setNextElement(2);
        assertThat(done).isDone();
        assertThat(trace).containsExactly(0, 1, 2);
    }

    @Test
    public void testTakeWhile_iteratorEnded() {
        TestIterator iterator = new TestIterator();
        List<Integer> trace = new ArrayList<>();
        ListenableFuture<?> done = iterators.takeWhile(iterator, element -> {
            trace.add(element);
            return element < 2;
        });
        iterator.setNextElement(0);
        assertThat(done).isNotDone();
        iterator.setNextElement(1);
        assertThat(done).isNotDone();
        iterator.setFinished();
        assertThat(done).isDone();
        assertThat(trace).containsExactly(0, 1);
    }

    @Test
    public void testTakeWhile_error() {
        TestIterator iterator = new TestIterator();
        RuntimeException thrown = new RuntimeException();
        ListenableFuture<?> done = iterators.takeWhile(iterator, element -> {
            throw thrown;
        });
        iterator.setNextElement(0);
        assertThatThrownBy(() -> Futures.getUnchecked(done)).hasCause(thrown);
    }

    @Test
    public void testFilter() {
        TestIterator iterator = new TestIterator();
        AsyncIterator<Integer> filtered = iterators.filter(iterator, x -> x % 2 != 0);
        iterator.setNextElement(0);
        assertThat(filtered.onHasNext()).isNotDone();
        iterator.setNextElement(1);
        assertThat(filtered.next()).isEqualTo(1);
        iterator.setNextElement(2);
        assertThat(filtered.onHasNext()).isNotDone();
        iterator.setNextElement(3);
        assertThat(filtered.next()).isEqualTo(3);
        iterator.setFinished();
        assertThat(filtered.onHasNext()).isDone();
        assertThat(filtered.hasNext()).isFalse();
    }

    @Test
    public void testNonBlockingPages() {
        TestIterator iterator = new TestIterator();
        AsyncIterator<List<Integer>> nonBlockingPages = iterators.nonBlockingPages(iterator);
        assertThat(nonBlockingPages.onHasNext()).isNotDone();
        iterator.setNextElement(0);
        assertThat(nonBlockingPages.onHasNext()).isDone();
        iterator.setNextElement(1);
        assertThat(nonBlockingPages.next()).containsExactly(0, 1);
        assertThat(nonBlockingPages.onHasNext()).isNotDone();
        iterator.setNextElement(2);
        iterator.setNextElement(3);
        iterator.setNextElement(4);
        iterator.setNextElement(5);
        assertThat(nonBlockingPages.next()).containsExactly(2, 3, 4, 5);
        iterator.setFinished();
        assertThat(nonBlockingPages.hasNext()).isFalse();
    }

    @Test
    public void testConcat() {

    }

    @Test
    public void testMergeSorted() {

    }

    @Test
    public void testTransform() {

    }

    @Test
    public void testTransformAsync() {

    }

    @Test
    public void testPeekingIterator() {
        TestIterator iterator = new TestIterator();
        PeekingAsyncIterator<Integer> peeking = iterators.peekingIterator(iterator);
        iterator.setNextElement(0);
        assertThat(peeking.peek()).isZero();
        iterator.setFinished();
        assertThat(peeking.hasNext()).isTrue();
        assertThat(peeking.next()).isZero();
        assertThat(peeking.hasNext()).isFalse();
    }

    private static final class TestIterator implements AsyncIterator<Integer> {
        private final Deque<Integer> nextElements = new ArrayDeque<>();
        private SettableFuture<Boolean> hasNext = SettableFuture.create();

        public void setFinished() {
            assertThat(hasNext.set(false)).isTrue();
        }

        public void setNextElement(Integer nextElement) {
            nextElements.add(nextElement);
            hasNext.set(true);
        }

        @Override
        public ListenableFuture<Boolean> onHasNext() {
            return hasNext;
        }

        @Override
        public boolean hasNext() {
            return Futures.getUnchecked(hasNext);
        }

        @Override
        public Integer next() {
            boolean hn = Futures.getUnchecked(hasNext);
            if (!hn) {
                throw new NoSuchElementException();
            }
            int next = nextElements.remove();
            if (nextElements.isEmpty()) {
                hasNext = SettableFuture.create();
            }
            return next;
        }
    }
}