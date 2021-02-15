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
package com.palantir.atlasdb.jepsen;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.jepsen.events.Event;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableCheckerResult.class)
@JsonDeserialize(as = ImmutableCheckerResult.class)
@Value.Immutable
public abstract class CheckerResult {
    public static CheckerResult combine(List<CheckerResult> results) {
        return ImmutableCheckerResult.builder()
                .valid(results.stream().allMatch(CheckerResult::valid))
                .errors(results.stream()
                        .flatMap(result -> result.errors().stream())
                        .collect(Collectors.toList()))
                .build();
    }

    public abstract boolean valid();

    public abstract List<Event> errors();
}
