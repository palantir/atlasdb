/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.google.common.collect.ForwardingIterator;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

public class TrackingIterator<T, I extends Iterator<T>> extends ForwardingIterator<T> {
    private final I delegate;
    private final ToLongFunction<T> measurer;
    private final Consumer<Long> tracker;

    public TrackingIterator(I delegate, Consumer<Long> tracker, ToLongFunction<T> measurer) {
        this.delegate = delegate;
        this.tracker = tracker;
        this.measurer = measurer;
    }

    @Override
    protected I delegate() {
        return delegate;
    }

    @Override
    public T next() {
        T result = delegate.next();
        tracker.accept(measurer.applyAsLong(result));
        return result;
    }
}
