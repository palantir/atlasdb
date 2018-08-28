/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import java.io.IOException;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.palantir.common.persist.Persistable;

@JsonDeserialize(as = ImmutableWriteReference.class)
@JsonSerialize(as = ImmutableWriteReference.class)
@Value.Immutable
public abstract class WriteReference implements Persistable {
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

    public static final WriteReference DUMMY = WriteReference.of(
            TableReference.createFromFullyQualifiedName("dum.my"),
            Cell.create(new byte[] {0}, new byte[] {0}), false);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new AfterburnerModule());

    public static final Hydrator<WriteReference> BYTES_HYDRATOR = input -> {
        try {
            return OBJECT_MAPPER.readValue(input, WriteReference.class);
        } catch (IOException e) {
            throw new RuntimeException("Exception hydrating object.");
        }
    };

    @Override
    public byte[] persistToBytes() {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Exception processing JSON", e);
        }
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
