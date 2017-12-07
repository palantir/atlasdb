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

package com.palantir.atlasdb.sweep.queue;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.keyvalue.api.Cell;

@Value.Immutable
@JsonSerialize(as=ImmutableWrite.class)
@JsonDeserialize(as=ImmutableWrite.class)
public interface Write {

    @JsonProperty("ts")
    long timestamp();

    @JsonProperty("c")
    Cell cell();

    @JsonProperty("d")
    boolean isTombstone();

    static Write of(Cell cell, boolean isTombstone, long timestamp) {
        return ImmutableWrite.builder().cell(cell).isTombstone(isTombstone).timestamp(timestamp).build();
    }


}
