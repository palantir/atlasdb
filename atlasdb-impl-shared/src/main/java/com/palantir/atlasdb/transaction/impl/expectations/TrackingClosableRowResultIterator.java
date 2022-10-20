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

import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ForwardingClosableIterator;
import java.util.function.Consumer;

public class TrackingClosableRowResultIterator extends ForwardingClosableIterator<RowResult<Value>> {
    ClosableIterator<RowResult<Value>> delegate;
    Consumer<Long> tracker;

    public TrackingClosableRowResultIterator(ClosableIterator<RowResult<Value>> delegate, Consumer<Long> tracker) {
        this.delegate = delegate;
        this.tracker = tracker;
    }

    @Override
    protected ClosableIterator<RowResult<Value>> delegate() {
        return delegate;
    }

    @Override
    public RowResult<Value> next() {
        RowResult<Value> result = delegate.next();
        tracker.accept(ExpectationsUtils.byteSize(result));
        return result;
    }
}
