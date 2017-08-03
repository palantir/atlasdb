/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.deepkin;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitable;

public class TransformedBatchingVisitable<T, Y> implements BatchingVisitable<Y> {
    private final BatchingVisitable<T> delegate;
    private final Function<T, Y> transform;

    public TransformedBatchingVisitable(BatchingVisitable<T> delegate, Function<T, Y> transform) {
        this.delegate = delegate;
        this.transform = transform;
    }

    @Override
    public <K extends Exception> boolean batchAccept(int batchSize, AbortingVisitor<? super List<Y>, K> v) throws K {
        return delegate.batchAccept(batchSize, batch -> v.visit(batch.stream().map(transform).collect(Collectors.toList())));
    }
}
