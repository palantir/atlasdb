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

import com.google.common.collect.ImmutableSet;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;


public class ClosableIterators {
    private ClosableIterators() {/* */}

    public static <T> ClosableIterator<T> wrap(final Iterator<? extends T> it) {
        return new EmptyClose<T>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }
            @Override
            public T next() {
                return it.next();
            }
            @Override
            public void remove() {
                it.remove();
            }
        };
    }

    private abstract static class EmptyClose<T> implements ClosableIterator<T> {
        @Override
        public void close() {
            // do nothing
        }
    }

    private static final ClosableIterator<?> EMPTY_IMMUTABLE_CLOSABLE_ITERATOR = wrap(ImmutableSet.of().iterator());
    @SuppressWarnings("unchecked")
    public static final <T> ClosableIterator<T> emptyImmutableClosableIterator() {
        return (ClosableIterator<T>) EMPTY_IMMUTABLE_CLOSABLE_ITERATOR;
    }

    public static <T> ClosableIterator<T> wrap(final Iterator<? extends T> it, final Closeable closable) {
        return new ClosableIterator<T>() {

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return it.next();
            }

            @Override
            public void remove() {
                it.remove();
            }

            @Override
            public void close() {
                try {
                    closable.close();
                } catch (IOException e) {
                    Throwables.throwUncheckedException(e);
                }
            }
        };
    }

}
