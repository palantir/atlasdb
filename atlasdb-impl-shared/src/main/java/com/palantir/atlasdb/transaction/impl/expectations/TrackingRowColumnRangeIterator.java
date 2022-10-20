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
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

public class TrackingRowColumnRangeIterator extends ForwardingIterator<Map.Entry<Cell, Value>>
        implements RowColumnRangeIterator {

    Iterator<Map.Entry<Cell, Value>> delegate;
    Consumer<Long> tracker;

    public TrackingRowColumnRangeIterator(Iterator<Map.Entry<Cell, Value>> delegate, Consumer<Long> tracker) {
        this.delegate = delegate;
        this.tracker = tracker;
    }

    @Override
    protected Iterator<Map.Entry<Cell, Value>> delegate() {
        return delegate;
    }

    @Override
    public Map.Entry<Cell, Value> next() {
        Map.Entry<Cell, Value> entry = delegate().next();
        tracker.accept(ExpectationsUtils.valueByCellEntryByteSize(entry));
        return entry;
    }
}
