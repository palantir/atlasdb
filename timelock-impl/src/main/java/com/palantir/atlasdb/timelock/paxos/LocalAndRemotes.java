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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
interface LocalAndRemotes<T> {
    @Value.Parameter
    T local();

    @Value.Parameter
    List<T> remotes();

    @Value.Derived
    @Value.Auxiliary
    default List<T> all() {
        return ImmutableList.<T>builder().addAll(remotes()).add(local()).build();
    }

    default <U> LocalAndRemotes<U> map(Function<T, U> mapper) {
        return ImmutableLocalAndRemotes.of(
                mapper.apply(local()), remotes().stream().map(mapper).collect(Collectors.toList()));
    }

    default LocalAndRemotes<T> enhanceRemotes(UnaryOperator<T> mapper) {
        return ImmutableLocalAndRemotes.of(
                local(), remotes().stream().map(mapper).collect(Collectors.toList()));
    }

    static <T> LocalAndRemotes<T> of(T local, List<T> remotes) {
        return ImmutableLocalAndRemotes.of(local, remotes);
    }
}
