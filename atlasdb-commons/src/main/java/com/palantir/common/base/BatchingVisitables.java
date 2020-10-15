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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.common.annotation.Inclusive;
import com.palantir.util.Mutable;
import com.palantir.util.Mutables;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

public class BatchingVisitables {
    private BatchingVisitables() {/**/}

    public static final int DEFAULT_BATCH_SIZE = 1000;
    @SuppressWarnings("unused") // Used in internal legacy code
    public static final int KEEP_ALL_BATCH_SIZE = 100000;

    public static <T> BatchingVisitableView<T> emptyBatchingVisitable() {
        return BatchingVisitableView.of(new BatchingVisitable<T>() {
            @Override
            public <K extends Exception> boolean batchAccept(int batchSize, AbortingVisitor<? super List<T>, K> v)
                    throws K {
                return true;
            }
        });
    }

    public static <T> BatchingVisitableView<T> singleton(final T t) {
        return BatchingVisitableView.of(new BatchingVisitable<T>() {
            @Override
            public <K extends Exception> boolean batchAccept(int batchSize, AbortingVisitor<? super List<T>, K> v)
                    throws K {
                return v.visit(ImmutableList.of(t));
            }
        });
    }

    public static long count(BatchingVisitable<?> visitable) {
        return count(visitable, 1000);
    }

    public static long count(BatchingVisitable<?> visitable, int batchSize) {
        return countInternal(visitable, batchSize);
    }

    private static <T> long countInternal(BatchingVisitable<T> visitable, int batchSize) {
        final long[] count = new long[1];
        visitable.batchAccept(batchSize,
                AbortingVisitors.<T, RuntimeException>batching(
                        item -> {
                            count[0]++;
                            return true;
                        }));
        return count[0];
    }

    public static <T> boolean isEmpty(BatchingVisitable<T> v) {
        return v.batchAccept(1, AbortingVisitors.<T, RuntimeException>batching(AbortingVisitors.<T>alwaysFalse()));
    }

    /**
     * @return Visitable containing elements from the start of the visitable
     * for which the predicate holds true. Once boundingPredicate returns false for an
     * element of visitable, no more elements will be included in the returned visitable.
     */
    public static <T> BatchingVisitableView<T> visitWhile(final BatchingVisitable<T> visitable,
            final Predicate<T> condition) {
        return BatchingVisitableView.of(new AbstractBatchingVisitable<T>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                    final ConsistentVisitor<T, K> v) throws K {
                visitable.batchAccept(batchSizeHint, batch -> {
                    for (T t : batch) {
                        if (!condition.apply(t)) {
                                return false;
                        }
                        boolean keepGoing = v.visitOne(t);
                        if (!keepGoing) {
                            return false;
                        }
                    }
                    return true;
                });
            }
        });
    }

    /**
     * @return the first element or null if the visitable is empty
     */
    @Nullable
    public static <T> T getFirst(BatchingVisitable<T> visitable) {
        return getFirst(visitable, null);
    }

    @Nullable
    public static <T> T getFirst(BatchingVisitable<T> visitable, @Nullable T defaultElement) {
        final Mutable<T> ret = Mutables.newMutable(defaultElement);
        visitable.batchAccept(1, AbortingVisitors.<T, RuntimeException>batching(item -> {
            ret.set(item);
            return false;
        }));
        return ret.get();
    }

    public static <T> boolean isEqual(BatchingVisitable<T> v, final Iterable<T> it) {
        return isEqual(v, it.iterator());
    }

    public static <T> boolean isEqual(BatchingVisitable<T> v, final Iterator<T> it) {
        boolean ret = v.batchAccept(DEFAULT_BATCH_SIZE, batch -> {
            Iterator<T> toMatch = Iterators.limit(it, batch.size());
            return Iterators.elementsEqual(toMatch, batch.iterator());
        });
        if (it.hasNext()) {
            return false;
        }
        return ret;
    }

    /**
     * This method will throw if there are nulls in the visitable.
     *
     * @return null if the visitable is empty, otherwise return the smallest
     *         value.
     */
    public static <T extends Comparable<? super T>> T getMax(BatchingVisitable<T> v) {
        return getMax(v, Ordering.natural(), null);
    }

    /**
     * This will return the first maximal element in the visitable. This method
     * takes a default element and will return that if the visitable is empty.
     * If the visitable is non-empty it will return the largest value in the
     * visitable.
     * <p>
     * A common way to use this would be to pass <code>null</code> as the
     * defaultElement and have the ordering throw on null elements so you know
     * the visitable doesn't have any nulls.
     */
    public static <T> T getMax(BatchingVisitable<T> v,
                               final Ordering<? super T> o,
                               @Nullable T defaultElement) {
        final Mutable<T> ret = Mutables.newMutable(defaultElement);
        v.batchAccept(
                DEFAULT_BATCH_SIZE,
                AbortingVisitors.<T, RuntimeException>batching(new AbortingVisitor<T, RuntimeException>() {
                    boolean hasSeenFirst = false;
                    @Override
                    public boolean visit(T item) throws RuntimeException {
                        if (hasSeenFirst) {
                            ret.set(o.max(ret.get(), item));
                        } else {
                            // Call o.max here so it will throw if item is null and this
                            // ordering hates on nulls.
                            ret.set(o.max(item, item));
                            hasSeenFirst = true;
                        }
                        return true;
                    }
                }));
        return ret.get();
    }

    /**
     * This method will throw if there are nulls in the visitable.
     *
     * @return null if the visitable is empty, otherwise return the smallest value.
     */
    public static <T extends Comparable<? super T>> T getMin(BatchingVisitable<T> v) {
        return getMin(v, Ordering.natural(), null);
    }

    /**
     * This will return the first maximal element in the visitable. This method
     * takes a default element and will return that if the visitable is empty.
     * If the visitable is non-empty it will return the largest value in the
     * visitable.
     * <p>
     * A common way to use this would be to pass <code>null</code> as the
     * defaultElement and have the ordering throw on null elements so you know
     * the visitable doesn't have any nulls.
     */
    public static <T> T getMin(BatchingVisitable<T> v,
                               final Ordering<? super T> o,
                               @Nullable T defaultElement) {
        final Mutable<T> ret = Mutables.newMutable(defaultElement);
        v.batchAccept(
                DEFAULT_BATCH_SIZE,
                AbortingVisitors.<T, RuntimeException>batching(new AbortingVisitor<T, RuntimeException>() {
                    boolean hasSeenFirst = false;
                    @Override
                    public boolean visit(T item) throws RuntimeException {
                        if (hasSeenFirst) {
                            ret.set(o.min(ret.get(), item));
                        } else {
                            // Call o.max here so it will throw if item is null and this
                            // ordering hates on nulls.
                            ret.set(o.min(item, item));
                            hasSeenFirst = true;
                        }
                        return true;
                    }
                }));
        return ret.get();
    }

    @Nullable
    public static <T> T getLast(BatchingVisitable<T> visitable) {
        return getLast(visitable, null);
    }

    @Nullable
    public static <T> T getLast(BatchingVisitable<T> visitable, @Nullable T defaultElement) {
        final Mutable<T> ret = Mutables.newMutable(defaultElement);
        visitable.batchAccept(DEFAULT_BATCH_SIZE,
                AbortingVisitors.<T, RuntimeException>batching(item -> {
                    ret.set(item);
                    return true;
                }));
        return ret.get();
    }

    public static <T> BatchingVisitableView<T> filter(BatchingVisitable<T> visitable, final Predicate<? super T> pred) {
        com.palantir.logsafe.Preconditions.checkNotNull(pred);
        return transformBatch(visitable, input -> ImmutableList.copyOf(Iterables.filter(input, pred)));
    }

    public static <F, T> BatchingVisitableView<T> transform(BatchingVisitable<F> visitable, final Function<? super F, ? extends T> f) {
        return transformBatch(visitable, from -> Lists.transform(from, f));
    }

    public static <F, T> BatchingVisitableView<T> transformBatch(final BatchingVisitable<F> visitable,
            final Function<? super List<F>, ? extends List<T>> f) {
        com.palantir.logsafe.Preconditions.checkNotNull(visitable);
        com.palantir.logsafe.Preconditions.checkNotNull(f);
        return BatchingVisitableView.of(new AbstractBatchingVisitable<T>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                                                                     final ConsistentVisitor<T, K> v) throws K {
                visitable.batchAccept(batchSizeHint, batch -> v.visit(f.apply(batch)));
            }
        });
    }

    public static <T> BatchingVisitableView<T> limit(final BatchingVisitable<T> visitable, final long limit) {
        com.palantir.logsafe.Preconditions.checkNotNull(visitable);
        com.palantir.logsafe.Preconditions.checkArgument(limit >= 0);
        if (limit == 0) {
            return emptyBatchingVisitable();
        }
        return BatchingVisitableView.of(new AbstractBatchingVisitable<T>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                                                                     final ConsistentVisitor<T, K> v) throws K {
                if (batchSizeHint > limit) {
                    batchSizeHint = (int) limit;
                }
                visitable.batchAccept(batchSizeHint, new AbortingVisitor<List<T>, K>() {
                    long visited = 0;
                    @Override
                    public boolean visit(List<T> batch) throws K {
                        for (T item : batch) {
                            if (!v.visitOne(item)) {
                                return false;
                            }
                            visited++;
                            if (visited >= limit) {
                                // Stop visiting early by returning false.
                                // The underlying ConsistentVisitor will still cause #batchAccept
                                // to return true.
                                return false;
                            }
                        }
                        return true;
                    }
                });
            }
        });
    }

    public static <T> BatchingVisitableView<T> skip(final BatchingVisitable<T> visitable, final long toSkip) {
        com.palantir.logsafe.Preconditions.checkNotNull(visitable);
        com.palantir.logsafe.Preconditions.checkArgument(toSkip >= 0);
        if (toSkip == 0) {
            return BatchingVisitableView.of(visitable);
        }
        return BatchingVisitableView.of(new AbstractBatchingVisitable<T>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                                                                        final ConsistentVisitor<T, K> v) throws K {
                visitable.batchAccept(batchSizeHint, new AbortingVisitor<List<T>, K>() {
                    long visited = 0;
                    @Override
                    public boolean visit(List<T> batch) throws K {
                        for (T item : batch) {
                            if (visited < toSkip) {
                                visited++;
                                continue;
                            }

                            if (!v.visitOne(item)) {
                                return false;
                            }
                        }
                        return true;
                    }
                });
            }
        });
    }

    /**
     * This is similar to the unix uniq command.
     * <p>
     * If there is a sequence of identical values, it will be replaced by one value.
     *
     * For example "AAABABBB" will become "ABAB"
     *
     * If the list passed in is sorted, then it will actually result in a list of unique elements
     * <p>
     * This uses {@link Objects#equal(Object, Object)} to do comparisons.
     * <p>
     * null is supported bug discouraged
     */
    public static <T> BatchingVisitableView<T> unique(final BatchingVisitable<T> visitable) {
        return uniqueOn(visitable, Functions.<T>identity());
    }

    public static <T> BatchingVisitableView<T> uniqueOn(final BatchingVisitable<T> visitable, final Function<T, ?> function) {
        com.palantir.logsafe.Preconditions.checkNotNull(visitable);
        com.palantir.logsafe.Preconditions.checkNotNull(function);
        return BatchingVisitableView.of(new AbstractBatchingVisitable<T>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                                                                        final ConsistentVisitor<T, K> v)
                    throws K {
                visitable.batchAccept(batchSizeHint, new AbortingVisitor<List<T>, K>() {
                    boolean hasVisitedFirst = false;
                    Object lastVisited = null;
                    @Override
                    public boolean visit(List<T> batch) throws K {
                        for (T item : batch) {

                            Object itemKey = function.apply(item);

                            if (!hasVisitedFirst || !Objects.equal(itemKey, lastVisited)) {
                                if (!v.visitOne(item)) {
                                    return false;
                                }
                                hasVisitedFirst = true;
                                lastVisited = itemKey;
                            }
                        }
                        return true;
                    }
                });
            }
        });
    }

    public static <T> List<T> copyToList(BatchingVisitable<T> v) {
        return BatchingVisitableView.of(v).immutableCopy();
    }

    public static <T> List<T> take(BatchingVisitable<T> v, int howMany) {
        return take(v, howMany, true);
    }

    public static <T> List<T> take(BatchingVisitable<T> v, final int howMany, final boolean includeFirst) {
        BatchingVisitableView<T> visitable = BatchingVisitableView.of(v);
        if (!includeFirst) {
            visitable = visitable.skip(1);
        }
        return visitable.limit(howMany).immutableCopy();
    }

    public static <T, TOKEN> TokenBackedBasicResultsPage<T, TOKEN> getFirstPage(BatchingVisitable<T> v,
                                                                                int numToVisitArg,
                                                                                Function<T, TOKEN> tokenExtractor) {
        Preconditions.checkArgument(numToVisitArg >= 0,
                "numToVisit cannot be negative.  Value was: %d", numToVisitArg);

        if (numToVisitArg == Integer.MAX_VALUE) {
            // prevent issue with overflow
            numToVisitArg--;
        }

        final int numToVisit = numToVisitArg + 1;
        ImmutableList<T> list = BatchingVisitableView.of(v).limit(numToVisit).immutableCopy();

        com.palantir.logsafe.Preconditions.checkState(list.size() <= numToVisit);
        if (list.size() >= numToVisit) {
            TOKEN token = tokenExtractor.apply(list.get(list.size() - 1));
            list = list.subList(0, numToVisit-1);
            return new SimpleTokenBackedResultsPage<T, TOKEN>(token, list, true);
        }

        return new SimpleTokenBackedResultsPage<T, TOKEN>(null, list, false);
    }

    public static <T> TokenBackedBasicResultsPage<T, T> getFirstPage(BatchingVisitable<T> v, int numToVisitArg) {
        return getFirstPage(v, numToVisitArg, Functions.<T>identity());
    }

    /**
     * This method will wrap the passed visitable so it is called with the passed pageSize when it is called.
     * <p>
     * This can be used to make the performance of batching visitables better.  One example of where this is useful
     * is if I just visit the results one at a time, but I know that I will visit 100 results, then I can save
     * a lot of potential round trips by hinting a page size of 100.
     */
    public static <T> BatchingVisitableView<T> hintPageSize(final BatchingVisitable<T> bv, final int pageSize) {
        com.palantir.logsafe.Preconditions.checkArgument(pageSize > 0);
        return BatchingVisitableView.of(new AbstractBatchingVisitable<T>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                                                                        ConsistentVisitor<T, K> v) throws K {
                bv.batchAccept(pageSize, v);
            }
        });
    }

    /**
     * This will wrap the passed visitor and return a more generic type.  Since Visitables just produce values,
     * their type can be made more generic because a Visitable&lt;Long&gt; can safely be cast to a Visitable&lt;Object&gt;.
     *
     * @see BatchingVisitableView for more helper methods
     */
    public static <T, S extends T> BatchingVisitable<T> wrap(final BatchingVisitable<S> visitable) {
        if (visitable == null) {
            return null;
        }
        return new BatchingVisitable<T>() {
            @Override
            public <K extends Exception> boolean batchAccept(int batchSize, AbortingVisitor<? super List<T>, K> v) throws K {
                // This cast is safe because an abortingVisitor can consume more specific values
                @SuppressWarnings("unchecked")
                AbortingVisitor<? super List<S>, K> specificVisitor = (AbortingVisitor<? super List<S>, K>) v;
                return visitable.batchAccept(batchSize, specificVisitor);
            }
        };
    }

    public static <T> BatchingVisitableView<T> concat(BatchingVisitable<? extends T>... inputs) {
        return concat(ImmutableList.copyOf(inputs));
    }

    public static <T> BatchingVisitableView<T> concat(final Iterable<? extends BatchingVisitable<? extends T>> inputs) {
        com.palantir.logsafe.Preconditions.checkNotNull(inputs);
        return BatchingVisitableView.of(new AbstractBatchingVisitable<T>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint, ConsistentVisitor<T, K> v) throws K {
                for (BatchingVisitable<? extends T> bv : inputs) {
                    // This is safe because cast is never passed to anything and it's function
                    // batchAccept is covariant
                    @SuppressWarnings("unchecked")
                    BatchingVisitable<T> cast = (BatchingVisitable<T>) bv;

                    if (!cast.batchAccept(batchSizeHint, v)) {
                        return;
                    }
                }
            }
        });
    }

    public static <T> BatchingVisitableView<T> flatten(final int outerBatchHint, final BatchingVisitable<? extends BatchingVisitable<? extends T>> inputs) {
        com.palantir.logsafe.Preconditions.checkNotNull(inputs);
        return BatchingVisitableView.of(new AbstractBatchingVisitable<T>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(final int batchSizeHint, final ConsistentVisitor<T, K> v) throws K {
                inputs.batchAccept(outerBatchHint,
                        (AbortingVisitor<List<? extends BatchingVisitable<? extends T>>, K>) bvs -> {
                            for (BatchingVisitable<? extends T> bv : bvs) {
                                // This is safe because cast is never passed to anything and it's function
                                // batchAccept is covariant
                                @SuppressWarnings("unchecked")
                                BatchingVisitable<T> cast = (BatchingVisitable<T>) bv;

                                if (!cast.batchAccept(batchSizeHint, v)) {
                                    return false;
                                }
                            }
                            return true;
                        });
            }
        });
    }

    public static BatchingVisitable<Long> getOrderedVisitableUsingSublists(
            final OrderedSublistProvider<Long> sublistProvider, @Inclusive final long startId) {
        return BatchingVisitables.getOrderedVisitableUsingSublists(sublistProvider, startId,
                Functions.<Long>identity());
    }

    public static <T> BatchingVisitable<T> getOrderedVisitableUsingSublists(
            final OrderedSublistProvider<T> sublistProvider, @Inclusive final long startId,
            final Function<T, Long> idFunction) {
        BatchingVisitable<T> visitableWithDuplicates = new AbstractBatchingVisitable<T>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                    ConsistentVisitor<T, K> v) throws K {
                long nextId = startId;
                boolean abortedOrFinished = false;
                while (!abortedOrFinished) {
                    List<T> resultBatchWithDuplicates =
                            sublistProvider.getBatchAllowDuplicates(nextId, batchSizeHint);
                    boolean abortedByVisitor = !v.visit(resultBatchWithDuplicates);
                    nextId = nextIdStart(resultBatchWithDuplicates, batchSizeHint, idFunction);
                    boolean finishedAllBatches = nextId == -1L;
                    abortedOrFinished = abortedByVisitor || finishedAllBatches;
                }
            }
        };
        return BatchingVisitables.unique(visitableWithDuplicates);
    }

    private static <T> long nextIdStart(List<T> recentlyVisited, long batchSizeHint, Function<T, Long> idFunction) {
        if (recentlyVisited.size() < batchSizeHint) {
            return -1L;
        }
        long cur = idFunction.apply(recentlyVisited.get(recentlyVisited.size() - 1));
        if (cur == Long.MAX_VALUE) {
            return -1L;
        }
        return cur + 1L;
    }
}
