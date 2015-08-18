/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Iterator;

import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.palantir.common.base.ClosableIterator;

public final class ClosablePeekingIterator<T> extends ForwardingIterator<T> implements
        ClosableIterator<T>, PeekingIterator<T> {

    public static <V> ClosablePeekingIterator<V> of(ClosableIterator<V> it) {
        return new ClosablePeekingIterator<V>(it);
    }

    private final ClosableIterator<T> ci;
    private final PeekingIterator<T> pi;

    private ClosablePeekingIterator(ClosableIterator<T> ci) {
        this.ci = ci;
        this.pi = Iterators.peekingIterator(ci);
    }

    @Override
    protected Iterator<T> delegate() {
        // This takes care of next(), hasNext() and remove().
        // All of these should be handled by the peeking
        // iterator.
        return pi;
    }

    @Override
    public T peek() {
        return pi.peek();
    }

    @Override
    public void close() {
        ci.close();
    }

}
