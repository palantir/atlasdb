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
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import java.util.Optional;

public class ConsensusForgettingPutUnlessExistsStore {
    private final KeyValueService keyValueService;
    private final TableReference tableReference;

    public ConsensusForgettingPutUnlessExistsStore(KeyValueService keyValueService, TableReference tableReference) {
        this.keyValueService = keyValueService;
        this.tableReference = tableReference;
    }

    public Optional<PutUnlessExistsState> get(Cell c) {
        return Optional.ofNullable(keyValueService
                        .get(tableReference, ImmutableMap.of(c, Long.MAX_VALUE))
                        .get(c))
                .map(Value::getContents)
                .map(PutUnlessExistsState::fromBytes);
    }

    public void putUnlessExists(Cell c, PutUnlessExistsState state) {
        keyValueService.putUnlessExists(tableReference, ImmutableMap.of(c, state.toByteArray()));
    }

    public void put(Cell c, PutUnlessExistsState state) {
        keyValueService.putToCasTable(tableReference, ImmutableMap.of(c, state.toByteArray()));
    }

    public void checkAndSet(Cell c, PutUnlessExistsState oldValue, PutUnlessExistsState newValue) {
        keyValueService.checkAndSet(
                CheckAndSetRequest.singleCell(tableReference, c, oldValue.toByteArray(), newValue.toByteArray()));
    }
}
