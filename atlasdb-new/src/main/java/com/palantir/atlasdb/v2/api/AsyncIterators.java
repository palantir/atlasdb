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

package com.palantir.atlasdb.v2.api;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

public final class AsyncIterators {
    private static final ListenableFuture<Boolean> TRUE_FUTURE = Futures.immediateFuture(true);

    private final Executor executor;

    public AsyncIterators(Executor executor) {
        this.executor = executor;
    }

    public <T> ListenableFuture<?> takeWhile(AsyncIterator<T> iterator, Predicate<? super T> stopIfFalse) {
        Futures.whenAllSucceed(iterator.onHasNext())
                .call(() -> {
                    if (!iterator.hasNext() || !stopIfFalse.test(iterator.next())) {
                        return Futures.immediateFuture(null);
                    }
                    return takeWhile(iterator, stopIfFalse);
                }, executor);
    }

    public <T> AsyncIterator<T> filter(AsyncIterator<T> iterator, Predicate<? super T> keepIfTrue) {
        return new AbstractAsyncIterator<T>() {
            @Override
            public ListenableFuture<T> computeNext() {
                return Futures.whenAllSucceed(iterator.onHasNext())
                        .callAsync(() -> {
                            if (!iterator.hasNext()) {
                                return endOfData();
                            }
                            T element = iterator.next();
                            if (keepIfTrue.test(element)) {
                                return Futures.immediateFuture(element);
                            }
                            return Futures.submitAsync(this::computeNext, executor);
                        }, executor);
            }
        };
    }

    public <T> AsyncIterator<List<T>> nonBlockingPages(AsyncIterator<T> iterator) {
        return new AsyncIterator<List<T>>() {
            @Override
            public ListenableFuture<Boolean> onHasNext() {
                return iterator.onHasNext();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public List<T> next() {
                ImmutableList.Builder<T> resultBuilder = ImmutableList.builder();
                resultBuilder.add(iterator.next());
                // handle max page size
                iterator.forEachRemainingWhileNonBlocking(resultBuilder::add);
                return resultBuilder.build();
            }
        };
    }

    public <T> AsyncIterator<T> concat(AsyncIterator<Iterator<T>> unfilteredIterator) {
        AsyncIterator<Iterator<T>> iterator = filter(unfilteredIterator, Iterator::hasNext);
        return new AsyncIterator<T>() {
            private Iterator<T> currentInner;

            @Override
            public ListenableFuture<Boolean> onHasNext() {
                if (innerHasNext()) {
                    return TRUE_FUTURE;
                }
                return iterator.onHasNext();
            }

            @Override
            public boolean hasNext() {
                return innerHasNext() || iterator.hasNext();
            }

            private boolean innerHasNext() {
                return currentInner != null && currentInner.hasNext();
            }

            @Override
            public T next() {
                if (innerHasNext()) {
                    return currentInner.next();
                }
                currentInner = iterator.next();
                return currentInner.next();
            }
        };
    }

    public <T> AsyncIterator<T> mergeSorted(
            Comparator<T> comparator,
            AsyncIterator<? extends T> asyncIterator,
            Iterator<? extends T> mergeIn,
            BinaryOperator<T> mergeFunction) {
        PeekingAsyncIterator<T> peekingAsync = peekingIterator(asyncIterator);
        PeekingIterator<T> peekingSync = Iterators.peekingIterator(mergeIn);
        return new AsyncIterator<T>() {
            @Override
            public ListenableFuture<Boolean> onHasNext() {
                return Futures.transform(peekingAsync.onHasNext(),
                        hasNext -> hasNext || peekingSync.hasNext(),
                        MoreExecutors.directExecutor());
            }

            @Override
            public boolean hasNext() {
                return peekingAsync.hasNext() || peekingSync.hasNext();
            }

            @Override
            public T next() {
                int comparison = comparator.compare(peekingAsync.peek(), peekingSync.peek());
                if (comparison > 0) {
                    return peekingSync.next();
                } else if (comparison == 0) {
                    return mergeFunction.apply(peekingAsync.next(), peekingSync.next());
                } else {
                    return peekingAsync.next();
                }
            }
        };
    }

    public <T, R> AsyncIterator<R> transform(AsyncIterator<? extends T> iterator, Function<T, R> transformer) {
        return new AsyncIterator<R>() {
            @Override
            public ListenableFuture<Boolean> onHasNext() {
                return iterator.onHasNext();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public R next() {
                return transformer.apply(iterator.next());
            }
        };
    }

    public <T, R> AsyncIterator<R> transformAsync(
            AsyncIterator<? extends T> iterator, AsyncFunction<T, R> transformer) {
        return new AbstractAsyncIterator<R>() {
            @Override
            public ListenableFuture<R> computeNext() {
                return Futures.whenAllSucceed(iterator.onHasNext())
                        .callAsync(() -> transformer.apply(iterator.next()), executor);
            }
        };
    }

    public <T> PeekingAsyncIterator<T> peekingIterator(AsyncIterator<? extends T> iterator) {
        return new PeekingAsyncIterator<T>() {
            private boolean hasPeeked = false;
            private T peekedElement;

            @Override
            public T peek() {
                if (!hasPeeked) {
                    peekedElement = iterator.next();
                    hasPeeked = true;
                }
                return peekedElement;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ListenableFuture<Boolean> onHasNext() {
                if (hasPeeked) {
                    return TRUE_FUTURE;
                } else {
                    return iterator.onHasNext();
                }
            }

            @Override
            public boolean hasNext() {
                return hasPeeked || iterator.hasNext();
            }

            @Override
            public T next() {
                if (hasPeeked) {
                    T result = peekedElement;
                    hasPeeked = false;
                    peekedElement = null;
                    return result;
                }
                return iterator.next();
            }
        };
    }

    private static abstract class AbstractAsyncIterator<T> implements PeekingAsyncIterator<T> {
        private State state = State.NOT_READY;
        private ListenableFuture<T> next;

        private enum State {
            READY,
            NOT_READY,
            DONE,
            FAILED,
        }

        public abstract ListenableFuture<T> computeNext();

        public final ListenableFuture<T> endOfData() {
            state = State.DONE;
            return Futures.immediateFuture(null);
        }

        @Override
        public ListenableFuture<Boolean> onHasNext() {

            return null;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        private static void await(ListenableFuture<?> future) {
            try {
                Uninterruptibles.getUninterruptibly(future);
            } catch (ExecutionException e) {}
        }

        private synchronized ListenableFuture<T> tryToComputeNext() {
            state = State.FAILED;
            ListenableFuture<T> computing = computeNext();
            next = Futures.transform(computing, result -> {
                if (state != State.DONE) {
                    state = State.READY;
                }
                return result;
            }, MoreExecutors.directExecutor());
            return next;
        }

        @Override
        public final T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            state = State.NOT_READY;
            T result = Futures.getUnchecked(next);
            next = null;
            return result;
        }

        @Override
        public final T peek() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return Futures.getUnchecked(next);
        }
    }
}
