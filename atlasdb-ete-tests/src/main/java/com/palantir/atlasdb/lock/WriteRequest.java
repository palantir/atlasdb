/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableWriteRequest.class)
@JsonDeserialize(as = ImmutableWriteRequest.class)
public interface WriteRequest {
    TransactionId id();

    Map<String, String> rows();

    static WriteRequest of(TransactionId id, String... rows) {
        return ImmutableWriteRequest.builder()
                .id(id)
                .rows(KeyedStream.of(Stream.of(rows)).collectToMap())
                .build();
    }

    static WriteRequest of(TransactionId id, Map<String, String> rows) {
        return ImmutableWriteRequest.builder().id(id).rows(rows).build();
    }
}
