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

package com.palantir.atlasdb.lock;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableWriteRequest.class)
@JsonDeserialize(as = ImmutableWriteRequest.class)
public interface ReadRequest {
    TransactionId id();

    Set<String> rows();

    static ReadRequest of(TransactionId id, String... rows) {
        return of(id, Stream.of(rows).collect(Collectors.toSet()));
    }

    static ReadRequest of(TransactionId id, Set<String> rows) {
        return ImmutableReadRequest.builder().id(id).rows(rows).build();
    }
}
