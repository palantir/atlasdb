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
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.palantir.logsafe.Preconditions;
import com.palantir.util.Pair;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class IterableUtils {
    private IterableUtils() {
        /* */
    }

    public static <T> Collection<T> toCollection(Iterable<T> iterable) {
        return new IterableCollection<T>(iterable, false);
    }

    public static <T> Collection<T> toRemovableCollection(Iterable<T> iterable) {
        return new IterableCollection<T>(iterable, true);
    }

    public static <T> Collection<T> toCollection(Iterable<T> iterable, int size) {
        return new IterableCollection<T>(iterable, size);
    }

    public static <T> Iterable<T> append(Iterable<? extends T> a, T b) {
        Preconditions.checkNotNull(a);
        return Iterables.concat(a, Collections.singleton(b));
    }

    public static <T> Collection<T> append(Collection<? extends T> a, T b) {
        Preconditions.checkNotNull(a);
        return toCollection(append((Iterable<? extends T>) a, b));
    }

    /**
     * Get first element or null if the iterable is empty.
     */
    public static <T> T getFirst(Iterable<T> items) {
        return IteratorUtils.getFirst(items.iterator());
    }

    /**
     * This can be safely used to cast an Iterable from one type to a super type.
     */
    public static <T> Iterable<T> wrap(final Iterable<? extends T> iterable) {
        if (iterable == null) {
            return null;
        }
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return IteratorUtils.wrap(iterable.iterator());
            }

            @Override
            public String toString() {
                return iterable.toString();
            }
        };
    }

    public static <T, U> Iterable<Pair<T, U>> zip(final Iterable<? extends T> it1, final Iterable<? extends U> it2) {
        return new Iterable<Pair<T, U>>() {
            @Override
            public Iterator<Pair<T, U>> iterator() {
                return IteratorUtils.zip(it1.iterator(), it2.iterator());
            }

            @Override
            public String toString() {
                return Iterables.toString(this);
            }
        };
    }

    public static <T, U> Iterable<Pair<T, U>> zip(
            final Iterable<? extends T> itT, final Function<T, U> transformationToU) {
        return zip(itT, Iterables.transform(itT, transformationToU));
    }

    static class IterableCollection<T> extends AbstractCollection<T> {
        private final Iterable<T> iterable;
        private final boolean allowRemove;
        private final Integer size;

        public IterableCollection(Iterable<T> iterable, boolean allowRemove) {
            this.allowRemove = allowRemove;
            this.iterable = iterable;
            this.size = null;
        }

        public IterableCollection(Iterable<T> iterable, int size) {
            this.allowRemove = false;
            this.iterable = iterable;
            this.size = size;
        }

        @Override
        public Iterator<T> iterator() {
            if (allowRemove) {
                return iterable.iterator();
            } else {
                return Iterators.unmodifiableIterator(iterable.iterator());
            }
        }

        @Override
        public int size() {
            if (size != null) {
                return size;
            }
            return Iterables.size(iterable);
        }
    }

    /**
     * Returns an iterable that applies {@code function} to each element of
     * {@code fromIterable}.
     *
     * <p>
     * The returned iterable's iterator supports {@code remove()} if the
     * provided iterator does. After a successful {@code remove()} call,
     * {@code fromIterable} no longer contains the corresponding element.
     *
     * <p>
     * NOTE: If the input {@code Iterable} is known to be a {@code List} or
     * other {@code Collection}, the returned iterable will also implement
     * {@code List} or {@code Collection} respectively and is beneficial for
     * efficiently copying the result to a new collection.
     */
    public static <F, T> Iterable<T> transform(
            final Iterable<F> fromIterable, final Function<? super F, ? extends T> function) {
        /*
         * Ideally this would be automatic from Guava's Iterables.transform but
         * it is not yet supported, see
         * https://code.google.com/p/guava-libraries/issues/detail?id=1413
         */
        Preconditions.checkNotNull(fromIterable);
        Preconditions.checkNotNull(function);
        if (fromIterable instanceof List) {
            return Lists.transform((List<F>) fromIterable, function);
        } else if (fromIterable instanceof Collection) {
            return cast(Collections2.transform((Collection<F>) fromIterable, function));
        } else {
            return Iterables.transform(fromIterable, function);
        }
    }

    /**
     * Used to avoid http://bugs.sun.com/view_bug.do?bug_id=6558557
     */
    @SuppressWarnings("unchecked")
    private static <T> Iterable<T> cast(Collection<? extends T> c) {
        return (Iterable<T>) c;
    }
}
