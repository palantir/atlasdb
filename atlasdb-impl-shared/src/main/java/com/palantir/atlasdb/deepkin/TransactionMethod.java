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

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Function;

import org.immutables.value.Value;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.common.base.BatchingVisitable;

@Value.Immutable
public interface TransactionMethod<T, V> {
    String name();
    List<CallArgument<?, ?>> callArguments();
    ResultTransform<T, V> resultTransform();

    default Map<String, Object> transformArguments(Object... arguments) {
        if (arguments.length != callArguments().size()) {
            throw new IllegalStateException("call got more arguments than is defined");
        }
        Map<String, Object> serialized = Maps.newHashMap();
        for (int i = 0; i < arguments.length; i++) {
            CallArgument arg = callArguments().get(i);
            serialized.put(arg.name(), arg.serializer().apply(arguments[i]));
        }
        return serialized;
    }

    TransactionMethod<SortedMap<byte[], RowResult<byte[]>>, SortedMap<byte[], RowResult<byte[]>>> GET_ROWS = ImmutableTransactionMethod.<SortedMap<byte[], RowResult<byte[]>>, SortedMap<byte[], RowResult<byte[]>>> builder()
            .name("getRows")
            .addCallArguments(
                    CallArgument.TABLE_REFERENCE,
                    CallArgument.ROWS,
                    CallArgument.COLUMN_RANGE_SELECTION
            )
            .resultTransform(ImmutableResultTransform.<SortedMap<byte[], RowResult<byte[]>>, SortedMap<byte[], RowResult<byte[]>>>builder()
                    .serializedType(new TypeToken<SortedMap<byte[], RowResult<byte[]>>>() {})
                    .deserializer(Function.identity())
                    .serializer(Function.identity())
                    .build()
            )
            .build();
    TransactionMethod<Map<byte[], CachedBatchingVisitable<AbstractMap.SimpleEntry<Cell, byte[]>>>, Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>>> GET_ROWS_COLUMN_RANGE = ImmutableTransactionMethod.<Map<byte[], CachedBatchingVisitable<AbstractMap.SimpleEntry<Cell, byte[]>>>, Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>>>builder()
            .name("getRowsColumnRange")
            .addCallArguments(
                    CallArgument.TABLE_REFERENCE,
                    CallArgument.ROWS,
                    CallArgument.COLUMN_RANGE_SELECTION
            )
            .resultTransform(ImmutableResultTransform.<Map<byte[], CachedBatchingVisitable<AbstractMap.SimpleEntry<Cell, byte[]>>>, Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>>>builder()
                    .serializedType(
                            new TypeToken<Map<byte[], CachedBatchingVisitable<AbstractMap.SimpleEntry<Cell, byte[]>>>>() {})
                    .serializer(result -> Maps.transformValues(
                            Maps.transformValues(
                                    result,
                                    v -> new TransformedBatchingVisitable<>(v, AbstractMap.SimpleEntry::new)
                            ),
                            CachedBatchingVisitable::cache
                    ))
                    .deserializer(result -> Maps.transformValues(
                            Maps.transformValues(
                                    result,
                                    v -> new TransformedBatchingVisitable<AbstractMap.SimpleEntry<Cell, byte[]>, Map.Entry<Cell, byte[]>>(v, e -> e)
                            ), v -> v)
                    )
                    .build()
            )
            .build();
    TransactionMethod<List<Map.Entry<Cell, byte[]>>, Iterator<Map.Entry<Cell, byte[]>>> GET_BATCHED_ROWS_COLUMN_RANGE = ImmutableTransactionMethod.<List<Map.Entry<Cell, byte[]>>, Iterator<Map.Entry<Cell, byte[]>>> builder()
            .name("getBatchedRowsColumnRange")
            .addCallArguments(
                    CallArgument.TABLE_REFERENCE,
                    CallArgument.ROWS,
                    CallArgument.COLUMN_RANGE_SELECTION,
                    CallArgument.BATCH_HINT
            )
            .resultTransform(ImmutableResultTransform.<List<Map.Entry<Cell, byte[]>>, Iterator<Map.Entry<Cell, byte[]>>> builder()
                    .serializedType(new TypeToken<List<Map.Entry<Cell, byte[]>>>() {})
                    .serializer(result ->
                            Lists.newArrayList(Iterators.transform(result, AbstractMap.SimpleEntry::new))
                    )
                    .deserializer(List::iterator)
                    .build()
            )
            .build();
    TransactionMethod<Map<Cell, byte[]>, Map<Cell, byte[]>> GET = ImmutableTransactionMethod.<Map<Cell, byte[]>, Map<Cell, byte[]>> builder()
            .name("get")
            .addCallArguments(
                    CallArgument.TABLE_REFERENCE,
                    CallArgument.CELLS
            )
            .resultTransform(ImmutableResultTransform.<Map<Cell, byte[]>, Map<Cell, byte[]>> builder()
                    .serializedType(new TypeToken<Map<Cell, byte[]>>() {})
                    .serializer(Function.identity())
                    .deserializer(Function.identity())
                    .build()
            )
            .build();
    TransactionMethod<CachedBatchingVisitable<RowResult<byte[]>>, BatchingVisitable<RowResult<byte[]>>> GET_RANGE = ImmutableTransactionMethod.<CachedBatchingVisitable<RowResult<byte[]>>, BatchingVisitable<RowResult<byte[]>>> builder()
            .name("getRange")
            .addCallArguments(
                    CallArgument.TABLE_REFERENCE,
                    CallArgument.RANGE_REQUEST
            )
            .resultTransform(ImmutableResultTransform.<CachedBatchingVisitable<RowResult<byte[]>>, BatchingVisitable<RowResult<byte[]>>> builder()
                    .serializedType(new TypeToken<CachedBatchingVisitable<RowResult<byte[]>>>() {})
                    .serializer(CachedBatchingVisitable::cache)
                    .deserializer(b -> b)
                    .build()
            )
            .build();
    TransactionMethod<List<CachedBatchingVisitable<RowResult<byte[]>>>, Iterable<BatchingVisitable<RowResult<byte[]>>>> GET_RANGES = ImmutableTransactionMethod.<List<CachedBatchingVisitable<RowResult<byte[]>>>, Iterable<BatchingVisitable<RowResult<byte[]>>>> builder()
            .name("getRanges")
            .addCallArguments(
                    CallArgument.TABLE_REFERENCE,
                    CallArgument.RANGE_REQUESTS
            )
            .resultTransform(ImmutableResultTransform.<List<CachedBatchingVisitable<RowResult<byte[]>>>, Iterable<BatchingVisitable<RowResult<byte[]>>>> builder()
                    .serializedType(new TypeToken<List<CachedBatchingVisitable<RowResult<byte[]>>>>() {})
                    .serializer(result -> Lists.newArrayList(Iterables.transform(result, CachedBatchingVisitable::cache)))
                    .deserializer(Lists::newArrayList)
                    .build()
            )
            .build();
    TransactionMethod<TransactionFailedException, TransactionFailedException> COMMIT = ImmutableTransactionMethod.<TransactionFailedException, TransactionFailedException> builder()
            .name("commit")
            .resultTransform(ImmutableResultTransform.<TransactionFailedException, TransactionFailedException> builder()
                    .serializedType(new TypeToken<TransactionFailedException>() {})
                    .serializer(Function.identity())
                    .deserializer(Function.identity())
                    .build()
            )
            .build();
    TransactionMethod<TransactionFailedException, TransactionFailedException> COMMIT_SERVICE = ImmutableTransactionMethod.<TransactionFailedException, TransactionFailedException> builder()
            .name("commitService")
            .resultTransform(ImmutableResultTransform.<TransactionFailedException, TransactionFailedException> builder()
                    .serializedType(new TypeToken<TransactionFailedException>() {})
                    .serializer(Function.identity())
                    .deserializer(Function.identity())
                    .build())
            .build();
    TransactionMethod<Long, Long> GET_TIMESTAMP = ImmutableTransactionMethod.<Long, Long> builder()
            .name("getTimestamp")
            .resultTransform(ImmutableResultTransform.<Long, Long> builder()
                    .serializedType(new TypeToken<Long>() {})
                    .serializer(Function.identity())
                    .deserializer(Function.identity())
                    .build()
            )
            .build();
}
