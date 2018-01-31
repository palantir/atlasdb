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

package com.palantir.atlasdb.transaction.api;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonDeserialize(as = ImmutableStaticReplayRepetition.class)
@JsonSerialize(as = ImmutableStaticReplayRepetition.class)
public interface StaticReplayRepetition extends ReplayRepetition {
    StaticReplayRepetition NO_REPETITIONS = StaticReplayRepetition.of(0);

    @Value.Default
    default int number() {
        return 1;
    }

    @Override
    default int repetitions(CapturedTransaction transaction) {
        return number();
    }

    static ImmutableStaticReplayRepetition.Builder builder() {
        return ImmutableStaticReplayRepetition.builder();
    }

    static StaticReplayRepetition of(int number) {
        return StaticReplayRepetition.builder().number(number).build();
    }
}
