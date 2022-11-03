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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public final class TrackingRowColumnRangeIterator
        extends TrackingIterator<Map.Entry<Cell, Value>, RowColumnRangeIterator> implements RowColumnRangeIterator {
    public TrackingRowColumnRangeIterator(
            RowColumnRangeIterator delegate, Consumer<Long> tracker, Function<Map.Entry<Cell, Value>, Long> measurer) {
        super(delegate, tracker, measurer);
    }
}
