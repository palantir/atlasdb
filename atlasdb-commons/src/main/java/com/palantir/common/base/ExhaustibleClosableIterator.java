/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

/**
 * An iterator which remembers if it has been totally consumed. hasNext sometimes does work.
 */
public class ExhaustibleClosableIterator<T> implements ClosableIterator<T> {
    private final ClosableIterator<T> delegate;
    private boolean exhausted = false;

    public ExhaustibleClosableIterator(ClosableIterator<T> delegate) {
        this.delegate = delegate;
    }

    public boolean isExhausted() {
        return exhausted;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = delegate.hasNext();
        exhausted |= hasNext;
        return hasNext;
    }

    @Override
    public T next() {
        return delegate.next();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
