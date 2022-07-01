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

package com.palantir.atlasdb.keyvalue.api;

import java.util.Map;
import org.immutables.value.Value;

/**
 * A request to be supplied to KeyValueService.multiCheckAndSet.
 */
// Todo(snanda): docs
@Value.Immutable
public interface MultiCheckAndSetRequest {
    @Value.Parameter
    TableReference tableRef();

    // todo(snanda): enforce that call cells are in one row
    @Value.Parameter
    byte[] rowName();

    // todo(snanda): need to enforce that old vals are a subset of new vals
    @Value.Parameter
    Map<Cell, byte[]> oldValueMap();

    @Value.Parameter
    Map<Cell, byte[]> newValueMap();

    static ImmutableMultiCheckAndSetRequest.Builder builder() {
        return ImmutableMultiCheckAndSetRequest.builder();
    }
}
