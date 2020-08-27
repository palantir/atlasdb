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

package com.palantir.paxos;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class PaxosResponses<T extends PaxosResponse> {
    abstract int quorum();
    abstract List<T> responses();

    public static <T extends PaxosResponse> PaxosResponses<T> of(int quorum, List<T> responses) {
        return ImmutablePaxosResponses.<T>builder()
                .quorum(quorum)
                .addAllResponses(responses)
                .build();
    }

    public List<T> get() {
        return responses();
    }

    public Stream<T> stream() {
        return responses().stream();
    }

    public <U extends PaxosResponse> PaxosResponses<U> map(Function<T, U> mapper) {
        return of(quorum(), responses().stream().map(mapper).collect(Collectors.toList()));
    }

    @Value.Derived
    int successes() {
        return (int) responses().stream().filter(PaxosResponse::isSuccessful).count();
    }

    @Value.Derived
    public boolean hasQuorum() {
        return successes() >= quorum();
    }

    @Value.Derived
    public int numberOfResponses() {
        return responses().size();
    }

    @Value.Derived
    PaxosQuorumStatus getQuorumResult() {
        if (hasQuorum()) {
            return PaxosQuorumStatus.QUORUM_AGREED;
        } else if (thereWereDisagreements()) {
            return PaxosQuorumStatus.SOME_DISAGREED;
        }
        return PaxosQuorumStatus.NO_QUORUM;
    }

    @Value.Derived
    protected boolean thereWereDisagreements() {
        return responses().stream().anyMatch(response -> !response.isSuccessful());
    }

}
