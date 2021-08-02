/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public final class Transactions3Service implements CombinedTransactionService {
    private final ObjectMapper objectMapper;
    private final KeyValueService kvs;
    private final TableReference transactionsTable;

    private Transactions3Service(ObjectMapper om, KeyValueService kvs, TableReference transactionsTable) {
        this.objectMapper = om;
        this.kvs = kvs;
        this.transactionsTable = transactionsTable;
    }

    // TODO (jkong): Smile? REALLY? This needs to be faster
    public static Transactions3Service create(KeyValueService kvs) {
        return new Transactions3Service(
                ObjectMappers.newSmileServerObjectMapper()
                        .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true),
                kvs,
                TransactionConstants.TRANSACTIONS3_TABLE);
    }

    @Override
    public Optional<TransactionCommittedState> getImmediateState(long startTimestamp) {
        Cell cell = getTransactionCell(startTimestamp);
        try {
            return Futures.transform(
                            kvs.getAsync(transactionsTable, ImmutableMap.of(cell, 1L)),
                            returnMap -> decodeTimestamp(startTimestamp, cell, returnMap),
                            MoreExecutors.directExecutor())
                    .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private Optional<TransactionCommittedState> decodeTimestamp(
            long _startTimestamp, Cell cell, Map<Cell, Value> returnMap) {
        Value value = returnMap.get(cell);
        return Optional.ofNullable(value).map(Value::getContents).map(bytes -> {
            try {
                return objectMapper.readValue(bytes, TransactionCommittedState.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public Map<Long, TransactionCommittedState> get(Iterable<Long> startTimestamps) {
        // TODO (jkong): Lol this is bad
        return KeyedStream.of(startTimestamps)
                .map(this::getImmediateState)
                .flatMap(Optional::stream)
                .collectToMap();
    }

    @Override
    public void putUnlessExists(long startTimestamp, TransactionCommittedState state) throws KeyAlreadyExistsException {
        Cell key = getTransactionCell(startTimestamp);
        kvs.putUnlessExists(transactionsTable, ImmutableMap.of(key, serializeUnchecked(state)));
    }

    @Override
    public void checkAndSet(
            long startTimestamp, TransactionCommittedState expected, TransactionCommittedState newProposal)
            throws KeyAlreadyExistsException {
        Cell key = getTransactionCell(startTimestamp);
        kvs.checkAndSet(CheckAndSetRequest.singleCell(
                transactionsTable, key, serializeUnchecked(expected), serializeUnchecked(newProposal)));
    }

    @Override
    public void close() {
        // Intentional
    }

    private byte[] serializeUnchecked(TransactionCommittedState state) {
        try {
            return objectMapper.writeValueAsBytes(state);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private Cell getTransactionCell(long startTimestamp) {
        return TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTimestamp);
    }
}
