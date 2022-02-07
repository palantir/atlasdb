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

package com.palantir.atlasdb.timelock.api;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableNamespace.class)
@JsonSerialize(as = ImmutableNamespace.class)
@Value.Immutable
public interface Namespace {
    @JsonValue
    String value();

    // For back-compatibility with conjure-generated Namespace
    default String get() {
        return value();
    }

    static Namespace valueOf(String value) {
        return of(value);
    }

    static Namespace of(String value) {
        return ImmutableNamespace.builder().value(value).build();
    }
}
