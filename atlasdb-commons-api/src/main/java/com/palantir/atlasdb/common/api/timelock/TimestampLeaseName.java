/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.common.api.timelock;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import java.util.Comparator;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableTimestampLeaseName.class)
@JsonSerialize(as = ImmutableTimestampLeaseName.class)
@Value.Immutable
@Safe
public interface TimestampLeaseName {
    Comparator<TimestampLeaseName> COMPARATOR = Comparator.comparing(TimestampLeaseName::name);

    String RESERVED_NAME_FOR_IMMUTABLE_TIMESTAMP = "ImmutableTimestamp";

    @JsonValue
    String name();

    @Value.Check
    default void check() {
        Preconditions.checkArgument(
                !name().startsWith(RESERVED_NAME_FOR_IMMUTABLE_TIMESTAMP),
                "Name must not be a reserved name",
                SafeArg.of("name", name()));
    }

    static TimestampLeaseName of(@Safe String name) {
        return ImmutableTimestampLeaseName.builder().name(name).build();
    }
}
