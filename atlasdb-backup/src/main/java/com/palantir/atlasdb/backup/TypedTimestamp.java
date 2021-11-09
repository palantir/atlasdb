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

package com.palantir.atlasdb.backup;

import org.immutables.value.Value;

@Value.Immutable
public interface TypedTimestamp {
    TimestampType type();

    long timestamp();

    static TypedTimestamp of(TimestampType type, long timestamp) {
        return builder().type(type).timestamp(timestamp).build();
    }

    static Builder builder() {
        return new Builder();
    }

    class Builder extends ImmutableTypedTimestamp.Builder {}
}
