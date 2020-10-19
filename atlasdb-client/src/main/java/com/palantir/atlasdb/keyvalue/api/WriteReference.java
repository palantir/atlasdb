/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableWriteReference.class)
@JsonSerialize(as = ImmutableWriteReference.class)
@Value.Immutable
public abstract class WriteReference {

    @JsonProperty("t")
    public abstract TableReference tableRef();
    @JsonProperty("c")
    public abstract Cell cell();
    @JsonProperty("d")
    public abstract boolean isTombstone();

    @Value.Lazy
    public CellReference cellReference() {
        return ImmutableCellReference.builder().tableRef(tableRef()).cell(cell()).build();
    }

    public static WriteReference tombstone(TableReference tableRef, Cell cell) {
        return WriteReference.of(tableRef, cell, true);
    }

    public static WriteReference write(TableReference tableRef, Cell cell) {
        return WriteReference.of(tableRef, cell, false);
    }

    public static WriteReference of(TableReference tableRef, Cell cell, boolean isTombstone) {
        return ImmutableWriteReference.builder().tableRef(tableRef).cell(cell).isTombstone(isTombstone).build();
    }
}
