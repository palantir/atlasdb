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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.common.visitor.Visitor;
import com.palantir.logsafe.Preconditions;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A wrapper for {@link BatchingVisitable} which adds support for common operations.
 *
 * @param <T> The contained object type.
 */
public abstract class BatchingVisitableView<T> extends ForwardingObject implements BatchingVisitable<T> {

    private BatchingVisitableView() {
        // don't expose the constructor publicly
    }

    @Override
    protected abstract BatchingVisitable<T> delegate();

    public static <T> BatchingVisitableView<T> of(final BatchingVisitable<T> underlyingVisitable) {
        Preconditions.checkNotNull(underlyingVisitable, "Cannot wrap a null visitable");
        return new BatchingVisitableView<T>() {
            @Override
            protected BatchingVisitable<T> delegate() {
                return BatchingVisitables.wrap(underlyingVisitable);
            }
        };
    }

    @Override
    public <K extends Exception> boolean batchAccept(
            int batchSize,
            AbortingVisitor<? super List<T>, K> visitor) throws K {
        return delegate().batchAccept(batchSize, visitor);
    }

    public BatchingVisitableView<T> concat(BatchingVisitable<? extends T>... inputs) {
        return BatchingVisitables.concat(Lists.asList(delegate(), inputs));
    }

    public long count() {
        return BatchingVisitables.count(delegate());
    }

    public BatchingVisitableView<T> filter(final Predicate<? super T> predicate) {
        Preconditions.checkNotNull(predicate, "Cannot filter using a null predicate");
        return BatchingVisitables.filter(delegate(), predicate);
    }

    public <U> BatchingVisitableView<T> filter(final Class<? extends U> type) {
        return filter(Predicates.instanceOf(type));
    }

    public BatchingVisitableView<T> limit(long limit) {
        return BatchingVisitables.limit(delegate(), limit);
    }

    public BatchingVisitableView<T> skip(long toSkip) {
        return BatchingVisitables.skip(delegate(), toSkip);
    }

    public BatchingVisitableView<T> unique() {
        return BatchingVisitables.unique(delegate());
    }

    public BatchingVisitableView<T> uniqueOn(Function<T, ?> function) {
        return BatchingVisitables.uniqueOn(delegate(), function);
    }

    public BatchingVisitableView<T> hintBatchSize(int batchSizeHint) {
        return BatchingVisitables.hintPageSize(delegate(), batchSizeHint);
    }

    public <U> BatchingVisitableView<U> transform(Function<? super T, ? extends U> fn) {
        Preconditions.checkNotNull(fn, "Cannot transform using a null function");
        return BatchingVisitables.transform(delegate(), fn);
    }

    /**
     * This can also be used to filter or multiply the input. It is common to
     * return a list of a smaller size than you pass in if you are filtering.
     * <p>
     * In the filtering case, the caller may only want one value, but you may
     * have to visit batches of size 100 to find that value. In this case you
     * should call .transformBatch and then call .hintBatchSize(100) so your
     * function will get 100 at a time even though the final consumer is only
     * requesting a batch of size 1.
     * A good example of this is in <code>BatchingVisitablesTest#testHintPageSize()</code>
     */
    public <U> BatchingVisitableView<U> transformBatch(Function<? super List<T>, ? extends List<U>> fn) {
        Preconditions.checkNotNull(fn, "Cannot transform using a null function");
        return BatchingVisitables.transformBatch(delegate(), fn);
    }

    public void forEach(int batchSize, final Visitor<T> visitor) {
        delegate().batchAccept(batchSize, batch -> {
            for (T item : batch) {
                visitor.visit(item);
            }
            return true;
        });
    }

    public void forEach(final Visitor<T> visitor) {
        forEach(BatchingVisitables.DEFAULT_BATCH_SIZE, visitor);
    }

    public BatchingVisitableView<T> visitWhile(Predicate<T> condition) {
        return BatchingVisitables.visitWhile(delegate(), condition);
    }

    /**
     * Gets the first element in the visitable.
     *
     * @return the first element or <code>null</code> if the visitable is empty
     */
    @Nullable
    public T getFirst() {
        return BatchingVisitables.getFirst(delegate());
    }

    /**
     * Gets the first element in the visitable.
     *
     * If a default value of <code>null</code> is wanted, then consider
     * calling {@link BatchingVisitableView#getFirst()} instead.
     *
     * @return the first element or <code>defaultElement</code> if the visitable is empty
     */
    @Nullable
    public T getFirst(@Nullable T defaultElement) {
        return BatchingVisitables.getFirst(delegate(), defaultElement);
    }

    public TokenBackedBasicResultsPage<T, T> getFirstPage(int numToVisitArg) {
        return BatchingVisitables.getFirstPage(delegate(), numToVisitArg);
    }

    /**
     * Gets the last element in the visitable.
     *
     * @return the last element or <code>null</code> if the visitable is empty
     */
    @Nullable
    public T getLast() {
        return BatchingVisitables.getLast(delegate());
    }

    /**
     * Gets the last element in the visitable.
     *
     * If a default value of <code>null</code> is wanted, then consider
     * calling {@link BatchingVisitableView#getLast()} instead.
     *
     * @return the last element or <code>defaultElement</code> if the visitable is empty
     */
    @Nullable
    public T getLast(@Nullable T defaultElement) {
        return BatchingVisitables.getLast(delegate(), defaultElement);
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
    public T getMax(Ordering<? super T> ordering, @Nullable T defaultElement) {
        return BatchingVisitables.getMax(delegate(), ordering, defaultElement);
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
    public T getMin(Ordering<? super T> ordering, @Nullable T defaultElement) {
        return BatchingVisitables.getMin(delegate(), ordering, defaultElement);
    }

    /**
     * Returns an immutable copy of the elements in this visitable.
     *
     * @throws NullPointerException if any elements in the visitable are null
     */
    public ImmutableList<T> immutableCopy() {
        final ImmutableList.Builder<T> builder = ImmutableList.builder();
        delegate().batchAccept(
                BatchingVisitables.DEFAULT_BATCH_SIZE,
                items -> {
                    builder.addAll(items);
                    return true;
                });
        return builder.build();
    }

    /**
     * Returns an immutable copy of the elements in this visitable.
     *
     * @throws NullPointerException if any elements in the visitable are null
     */
    public ImmutableSet<T> immutableSetCopy() {
        final ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        delegate().batchAccept(
                BatchingVisitables.DEFAULT_BATCH_SIZE,
                items -> {
                    builder.addAll(items);
                    return true;
                });
        return builder.build();
    }

    /**
     * Copies the elements in this visitable into an existing collection. This method has
     * equivalent behaviour to {@code Iterables.addAll(collection, this)}.
     *
     * @return a reference to {@code collection}, for convenience
     */
    public <S extends Collection<? super T>> S copyInto(final S collection) {
        Preconditions.checkNotNull(collection, "Cannot copy the visitable into a null collection");
        delegate().batchAccept(
                BatchingVisitables.DEFAULT_BATCH_SIZE,
                items -> {
                    collection.addAll(items);
                    return true;
                });
        return collection;
    }

    /**
     * Returns {@code true} iff one or more elements satisfy the predicate.
     */
    public boolean any(final Predicate<? super T> predicate) {
        Preconditions.checkNotNull(predicate, "Cannot check against a null predicate");
        return !delegate().batchAccept(
                BatchingVisitables.DEFAULT_BATCH_SIZE,
                items -> {
                    for (T t : items) {
                        if (predicate.apply(t)) {
                            return false;
                        }
                    }
                    return true;
                });
    }

    /**
     * Returns {@code true} iff every element satisfies the
     * predicate. If empty, {@code true} is returned.
     */
    public boolean all(final Predicate<? super T> predicate) {
        Preconditions.checkNotNull(predicate, "Cannot check against a null predicate");
        return delegate().batchAccept(
                BatchingVisitables.DEFAULT_BATCH_SIZE,
                items -> {
                    for (T t : items) {
                        if (!predicate.apply(t)) {
                            return false;
                        }
                    }
                    return true;
                });
    }

    public boolean isEmpty() {
        return BatchingVisitables.isEmpty(delegate());
    }

    public boolean isEqual(Iterable<T> it) {
        return BatchingVisitables.isEqual(delegate(), it);
    }

    public boolean isEqual(Iterator<T> it) {
        return BatchingVisitables.isEqual(delegate(), it);
    }

    public long size() {
        return BatchingVisitables.count(delegate());
    }

    public T find(Predicate<? super T> predicate, @Nullable T defaultValue) {
        return filter(predicate).getFirst(defaultValue);
    }

}
