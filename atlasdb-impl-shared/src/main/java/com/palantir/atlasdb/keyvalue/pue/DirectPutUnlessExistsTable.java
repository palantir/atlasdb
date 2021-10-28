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
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import java.util.Map;
import java.util.Optional;

public class DirectPutUnlessExistsTable<T> implements PutUnlessExistsTable<T> {
    private final KeyValueService keyValueService;
    private final TableReference tableReference;
    private final ValueSerializers<T> valueSerializers;

    public DirectPutUnlessExistsTable(
            KeyValueService keyValueService, TableReference tableReference, ValueSerializers<T> valueSerializers) {
        this.keyValueService = keyValueService;
        this.tableReference = tableReference;
        this.valueSerializers = valueSerializers;
    }

    @Override
    public T get(Cell c) {
        Map<Cell, Value> resultMap = keyValueService.get(tableReference, ImmutableMap.of(c, Long.MAX_VALUE));
        return Optional.ofNullable(resultMap.get(c))
                .map(Value::getContents)
                .map(bytes -> valueSerializers.byteDeserializer().apply(bytes))
                .orElse(null);
    }

    @Override
    public void putUnlessExists(Cell c, T value) throws KeyAlreadyExistsException {
        keyValueService.putUnlessExists(
                tableReference,
                ImmutableMap.of(c, valueSerializers.byteSerializer().apply(value)));
    }
}
