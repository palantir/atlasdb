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

package com.palantir.atlasdb.timelock.paxos;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.immutables.value.Value;

import com.palantir.common.streams.KeyedStream;

@Value.Immutable
public interface WithDedicatedExecutor<T> {
    T service();
    ExecutorService executor();

    default <U> WithDedicatedExecutor<U> transformService(Function<T, U> transform) {
        return WithDedicatedExecutor.of(transform.apply(service()), executor());
    }

    static <T> WithDedicatedExecutor<T> of(T service, ExecutorService executor) {
        return ImmutableWithDedicatedExecutor.<T>builder()
                .service(service)
                .executor(executor)
                .build();
    }

    static <T> Map<T, ExecutorService> convert(Collection<WithDedicatedExecutor<T>> services) {
        return KeyedStream.of(services)
                .map(WithDedicatedExecutor::executor)
                .mapKeys(WithDedicatedExecutor::service)
                .collectToMap();
    }
}
