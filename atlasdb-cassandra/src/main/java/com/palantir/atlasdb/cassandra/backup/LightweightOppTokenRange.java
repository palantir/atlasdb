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

package com.palantir.atlasdb.cassandra.backup;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import java.util.Optional;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableLightweightOppTokenRange.class)
@JsonSerialize(as = ImmutableLightweightOppTokenRange.class)
@Value.Immutable
public interface LightweightOppTokenRange {
    Optional<LightweightOppToken> left();

    Optional<LightweightOppToken> right();

    static LightweightOppTokenRange of(LightweightOppToken left, LightweightOppToken right) {
        return builder().left(left).right(right).build();
    }

    class Builder extends ImmutableLightweightOppTokenRange.Builder {}

    static Builder builder() {
        return new Builder();
    }
}
