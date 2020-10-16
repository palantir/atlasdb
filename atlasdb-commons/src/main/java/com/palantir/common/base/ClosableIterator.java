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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface ClosableIterator<T> extends Iterator<T>, Closeable {
    @Override
    default void close() {}

    default <U> ClosableIterator<U> map(Function<T, U> mapper) {
        return ClosableIterators.wrap(Iterators.transform(this, mapper::apply));
    }

    default <U> ClosableIterator<U> flatMap(Function<T, Collection<U>> mapper) {
        return ClosableIterators.wrap(
                stream().flatMap(obj -> mapper.apply(obj).stream()).iterator(), this);
    }

    default ClosableIterator<T> stopWhen(Predicate<T> shouldStop) {
        PeekingIterator<T> peekingIterator = Iterators.peekingIterator(this);
        return new ClosableIterator<T>() {

            @Override
            public boolean hasNext() {
                return peekingIterator.hasNext() && !shouldStop.test(peekingIterator.peek());
            }

            @Override
            public T next() {
                return peekingIterator.next();
            }
        };
    }

    default Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), false);
    }
}
