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

import java.util.Iterator;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.v2.api.api.AsyncIterator;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

public abstract class TransformingReader<In extends NewValue, Out extends NewValue> implements Reader<Out> {
    private final Reader<In> input;
    private final AsyncIterators iterators;

    protected TransformingReader(Reader<In> input, AsyncIterators iterators) {
        this.input = input;
        this.iterators = iterators;
    }

    @Override
    public final AsyncIterator<Out> scan(TransactionState state, ScanDefinition definition) {
        AsyncIterator<In> read = input.scan(state, definition);
        AsyncIterator<List<In>> buffered = iterators.nonBlockingPages(read);
        return iterators.concat(iterators.transformAsync(
                buffered, page -> transformPage(state, definition, page)));
    }

    protected abstract ListenableFuture<Iterator<Out>> transformPage(
            TransactionState state, ScanDefinition definition, List<In> page);
}
