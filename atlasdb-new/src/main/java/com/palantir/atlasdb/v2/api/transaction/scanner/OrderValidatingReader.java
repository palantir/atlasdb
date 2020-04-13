/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.transaction.scanner;

import static com.palantir.logsafe.Preconditions.checkState;

import java.util.Comparator;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.v2.api.api.AsyncIterator;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

public final class OrderValidatingReader<T extends NewValue> implements Reader<T> {
    private final Reader<T> delegate;

    public OrderValidatingReader(Reader<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public AsyncIterator<T> scan(TransactionState state, ScanDefinition definition) {
        Comparator<NewValue> comparator = definition.filter().toValueComparator();
        AsyncIterator<T> scan = delegate.scan(state, definition);
        return new AsyncIterator<T>() {
            private boolean failed = false;
            private T lastElement;

            @Override
            public ListenableFuture<Boolean> onHasNext() {
                return scan.onHasNext();
            }

            @Override
            public boolean hasNext() {
                return scan.hasNext();
            }

            @Override
            public T next() {
                T nextElement = scan.next();
                if (lastElement != null) {
                    failed |= comparator.compare(lastElement, nextElement) >= 0;
                }
                lastElement = nextElement;
                checkState(!failed, "ordering bug");
                return nextElement;
            }
        };
    }
}
