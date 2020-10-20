/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableWithSeq.class)
@JsonSerialize(as = ImmutableWithSeq.class)
public interface WithSeq<T> {

    @Value.Parameter
    long seq();

    @Value.Parameter
    T value();

    default <U> WithSeq<U> map(BiFunction<T, Long, U> mapper) {
        return WithSeq.of(mapper.apply(value(), seq()), seq());
    }

    default <U> WithSeq<U> map(Function<T, U> mapper) {
        return WithSeq.of(mapper.apply(value()), seq());
    }

    default <U> WithSeq<U> withNewValue(U newValue) {
        return WithSeq.of(newValue, seq());
    }

    static <T> WithSeq<T> of(T value, long seq) {
        return ImmutableWithSeq.of(seq, value);
    }
}
