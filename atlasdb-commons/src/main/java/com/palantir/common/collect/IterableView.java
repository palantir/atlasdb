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
package com.palantir.common.collect;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.logsafe.Preconditions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * A wrapper for an iterable that gives it super chaining powers. And a nice toString.
 *
 * @author carrino
 */
public abstract class IterableView<T> extends ForwardingObject implements Iterable<T> {

    @Override
    protected abstract Iterable<? extends T> delegate();

    public static <T> IterableView<T> of(final Iterable<? extends T> it) {
        return new IterableView<T>() {
            @Override
            protected Iterable<? extends T> delegate() {
                return it;
            }
        };
    }

    public static <T> IterableView<T> of(T... elements) {
        return of(Arrays.asList(elements));
    }

    public IterableView<T> concat(Iterable<? extends T>... inputs) {
        return of(Iterables.concat(Lists.asList(delegate(), inputs)));
    }

    public IterableView<T> concat(Iterable<? extends T> b, Iterable<? extends T> c) {
        Preconditions.checkNotNull(b);
        Preconditions.checkNotNull(c);
        return of(Iterables.concat(ImmutableList.of(delegate(), b, c)));
    }

    public IterableView<T> concat(Iterable<? extends T> b) {
        return of(Iterables.concat(delegate(), b));
    }

    public IterableView<T> filter(Predicate<? super T> predicate) {
        return of(Iterables.filter(delegate(), predicate));
    }

    public <U> IterableView<U> filter(final Class<? extends U> type) {
        return of(Iterables.filter(delegate(), type));
    }

    public <U> IterableView<U> transform(Function<? super T, ? extends U> function) {
        /*
         * Depending on the underlying delegate type, use Lists or Collections2
         * transform implementation to allow for more efficient sized
         * collections when constructing a collection from the transformed
         * result.
         */
        if (delegate() instanceof List) {
            return of(Lists.transform((List<T>) delegate(), function));
        } else if (delegate() instanceof Collection) {
            return of(Collections2.transform((Collection<T>) delegate(), function));
        }
        return of(Iterables.transform(delegate(), function));
    }

    public IterableView<T> skip(int numberToSkip) {
        return of(Iterables.skip(delegate(), numberToSkip));
    }

    public IterableView<T> limit(int limitSize) {
        return of(Iterables.limit(delegate(), limitSize));
    }

    public IterableView<T> consumingIterable() {
        return of(Iterables.consumingIterable(delegate()));
    }

    public IterableView<T> cycle() {
        return of(Iterables.cycle(delegate()));
    }

    public IterableView<T> unmodifiableIterable() {
        return of(Iterables.unmodifiableIterable(delegate()));
    }

    public IterableView<List<T>> partition(final int size) {
        if (delegate() instanceof List) {
            /*
             * Use the more efficient Lists.partition which utilizes sublists
             * without allocating new lists for the returned partitions.
             */
            return of(Lists.partition((List<T>) delegate(), size));
        }

        return of(Iterables.partition(castAsIterable(), size));
    }

    public boolean contains(@Nullable Object element) {
        return Iterables.contains(delegate(), element);
    }

    public T find(Predicate<? super T> predicate) {
        return Iterables.find(delegate(), predicate);
    }

    public int frequency(@Nullable Object element) {
        return Iterables.frequency(delegate(), element);
    }

    public int size() {
        return Iterables.size(delegate());
    }

    public boolean all(Predicate<? super T> predicate) {
        return Iterables.all(delegate(), predicate);
    }

    public boolean any(Predicate<? super T> predicate) {
        return Iterables.any(delegate(), predicate);
    }

    public boolean isEmpty() {
        return Iterables.isEmpty(delegate());
    }

    public T get(int position) {
        return Iterables.get(delegate(), position);
    }

    public T get(int position, @Nullable T defaultValue) {
        return Iterables.get(castAsIterable(), position, defaultValue);
    }

    public T getLast() {
        return Iterables.getLast(delegate());
    }

    public T getLast(@Nullable T defaultValue) {
        return Iterables.getLast(castAsIterable(), defaultValue);
    }

    public T getOnlyElement() {
        return Iterables.getOnlyElement(delegate());
    }

    public T getOnlyElement(@Nullable T defaultValue) {
        return Iterables.getOnlyElement(castAsIterable(), defaultValue);
    }

    public T getFirst() {
        return IterableUtils.getFirst(delegate());
    }

    /**
     * Returns an immutable copy of the current contents of this iterable view. Does not support
     * null elements.
     */
    public ImmutableList<T> immutableCopy() {
        return ImmutableList.copyOf(delegate());
    }

    /**
     * Returns an immutable copy of the current contents of this iterable view. Does not support
     * null elements.
     */
    public ImmutableSet<T> immutableSetCopy() {
        return ImmutableSet.copyOf(delegate());
    }

    /**
     * Copies the current contents of this set view into an existing collection. This method has
     * equivalent behavior to {@code Iterables.addAll(collection, this)}.
     * @return a reference to {@code set}, for convenience
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <S extends Collection<? super T>> S copyInto(S collection) {
        Iterables.addAll((Collection) collection, delegate());
        return collection;
    }

    @Override
    public Iterator<T> iterator() {
        return IteratorUtils.wrap(delegate().iterator());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Spliterator<T> spliterator() {
        return (Spliterator<T>) delegate().spliterator();
    }

    /**
     * @return a sequential {@code Stream} with the contents as its source.
     */
    @SuppressWarnings("unchecked")
    public Stream<T> stream() {
        if (delegate() instanceof Collection) {
            return ((Collection<T>) delegate()).stream();
        }
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public String toString() {
        return Iterables.toString(delegate());
    }

    /**
     * Casts the {@link #delegate()} as an {@link Iterable} of T to avoid type
     * warnings due to type erasure.
     */
    @SuppressWarnings("unchecked")
    private Iterable<T> castAsIterable() {
        return (Iterable<T>)delegate();
    }

}
