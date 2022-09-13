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

public final class ClosableIterators {
    private ClosableIterators() {
        /* */
    }

    /**
     * @deprecated Use the explicit `wrapWithEmptyClose` instead. This helps avoid accidental unwrapping.
     */
    @Deprecated
    public static <T> ClosableIterator<T> wrap(final Iterator<? extends T> it) {
        return wrapWithEmptyClose(it);
    }

    public static <T> ClosableIterator<T> wrapWithEmptyClose(final Iterator<? extends T> it) {
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

    private static final ClosableIterator<?> EMPTY_IMMUTABLE_CLOSABLE_ITERATOR =
            wrapWithEmptyClose(ImmutableSet.of().iterator());

    @SuppressWarnings("unchecked")
    public static <T> ClosableIterator<T> emptyImmutableClosableIterator() {
        return (ClosableIterator<T>) EMPTY_IMMUTABLE_CLOSABLE_ITERATOR;
    }

    /**
     * Run the on close after the original close method. The additional close will be run even if the original close
     * method fails.
     */
    public static <T> ClosableIterator<T> appendOnClose(ClosableIterator<T> closableIterator, final Closeable onClose) {
        return new ClosableIterator<T>() {
            @Override
            public boolean hasNext() {
                return closableIterator.hasNext();
            }

            @Override
            public T next() {
                return closableIterator.next();
            }

            @Override
            public void remove() {
                closableIterator.remove();
            }

            @Override
            public void close() {
                try {
                    closableIterator.close();
                } finally {
                    try {
                        onClose.close();
                    } catch (IOException e) {
                        Throwables.throwUncheckedException(e);
                    }
                }
            }
        };
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
