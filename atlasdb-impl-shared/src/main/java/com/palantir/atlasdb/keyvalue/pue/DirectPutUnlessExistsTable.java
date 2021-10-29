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

package com.palantir.atlasdb.keyvalue.pue;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;

public class DirectPutUnlessExistsTable implements PutUnlessExistsTable {
    private final KeyValueService keyValueService;
    private final TableReference tableReference;

    public DirectPutUnlessExistsTable(
            KeyValueService keyValueService, TableReference tableReference) {
        this.keyValueService = keyValueService;
        this.tableReference = tableReference;
    }

    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell c) {
        Map<Cell, Value> resultMap = keyValueService.get(tableReference, ImmutableMap.of(c, Long.MAX_VALUE));
        return Futures.immediateFuture(Optional.ofNullable(resultMap.get(c))
                .map(Value::getContents));
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> get(Iterable<Cell> cells) {
        return Futures.immediateFuture(
                KeyedStream.stream(
                keyValueService.get(tableReference,
                        KeyedStream.of(cells).map(_unused -> Long.MAX_VALUE).collectToMap()))
                        .map(Value::getContents)
                        .collectToMap()
        );
    }

    @Override
    public void putUnlessExists(Cell c, byte[] value) throws KeyAlreadyExistsException {
        keyValueService.putUnlessExists(
                tableReference,
                ImmutableMap.of(c, value));
    }
}
