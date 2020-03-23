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

package com.palantir.paxos;

import java.time.Instant;
import java.util.List;
import java.util.function.Function;

import org.derive4j.Data;

public interface PaxosExecutionEnvironment<S> {

    int numberOfServices();

    <R extends PaxosResponse> ExecutionContext<S, R> execute(Function<S, R> function);

    <T> PaxosExecutionEnvironment<T> map(Function<S, T> mapper);

    interface ExecutionContext<S, R extends PaxosResponse> {
        Result<S, R> awaitNextResult(Instant deadline) throws InterruptedException;
        ExecutionContext<S, R> withExistingResults(List<Result<S, R>> existingResults);
        void cancel();
    }

    @Data
    abstract class Result<S, R> {
        interface Cases<S, R, V> {
            V success(S service, R response);
            V deadlineExceeded();
            V failure(Throwable throwable);
        }

        public abstract <RR> RR match(Cases<S, R, RR> cases);
    }

}
