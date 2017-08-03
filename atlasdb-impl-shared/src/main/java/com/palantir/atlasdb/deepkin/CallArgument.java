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
import java.util.Set;
import java.util.function.Function;

import org.immutables.value.Value;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;

@Value.Immutable
public interface CallArgument<T, V> {
    String name();
    TypeToken<V> serializedType();
    Function<T, V> serializer();

    CallArgument<TableReference, TableReference> TABLE_REFERENCE = ImmutableCallArgument.<TableReference, TableReference> builder()
            .name("tableRef")
            .serializedType(new TypeToken<TableReference>(){})
            .serializer(Function.identity())
            .build();
    CallArgument<Iterable<byte[]>, List<byte[]>> ROWS = ImmutableCallArgument.<Iterable<byte[]>, List<byte[]>> builder()
            .name("rows")
            .serializedType(new TypeToken<List<byte[]>>() {})
            .serializer(Lists::newLinkedList)
            .build();
    CallArgument<BatchColumnRangeSelection, BatchColumnRangeSelection> COLUMN_RANGE_SELECTION = ImmutableCallArgument.<BatchColumnRangeSelection, BatchColumnRangeSelection> builder()
            .name("columnRangeSelection")
            .serializedType(new TypeToken<BatchColumnRangeSelection>() {})
            .serializer(Function.identity())
            .build();
    CallArgument<Integer, Integer> BATCH_HINT = ImmutableCallArgument.<Integer, Integer>builder()
            .name("batchHint")
            .serializedType(new TypeToken<Integer>() {})
            .serializer(Function.identity())
            .build();
    CallArgument<Set<Cell>, Set<Cell>> CELLS = ImmutableCallArgument.<Set<Cell>, Set<Cell>> builder()
            .name("cells")
            .serializedType(new TypeToken<Set<Cell>>() {})
            .serializer(Function.identity())
            .build();
    CallArgument<RangeRequest, RangeRequest> RANGE_REQUEST = ImmutableCallArgument.<RangeRequest, RangeRequest> builder()
            .name("rangeRequest")
            .serializedType(new TypeToken<RangeRequest>() {})
            .serializer(Function.identity())
            .build();
    CallArgument<List<RangeRequest>, Iterable<RangeRequest>> RANGE_REQUESTS = ImmutableCallArgument.<List<RangeRequest>, Iterable<RangeRequest>> builder()
            .name("rangeRequests")
            .serializedType(new TypeToken<Iterable<RangeRequest>>() {})
            .serializer(Lists::newArrayList)
            .build();
}
