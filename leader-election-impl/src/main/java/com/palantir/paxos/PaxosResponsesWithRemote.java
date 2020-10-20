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

import com.google.common.collect.ImmutableList;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.function.Function;
import org.immutables.value.Value;

@Value.Immutable
public abstract class PaxosResponsesWithRemote<S, T extends PaxosResponse> {

    abstract int quorum();

    public abstract Map<S, T> responses();

    public static <S, T extends PaxosResponse> PaxosResponsesWithRemote<S, T> of(int quorum, Map<S, T> responses) {
        return ImmutablePaxosResponsesWithRemote.<S, T>builder()
                .quorum(quorum)
                .putAllResponses(responses)
                .build();
    }

    public KeyedStream<S, T> stream() {
        return KeyedStream.stream(responses());
    }

    public <U extends PaxosResponse> PaxosResponsesWithRemote<S, U> map(Function<T, U> mapper) {
        return of(quorum(), KeyedStream.stream(responses()).map(mapper).collectToMap());
    }

    @Value.Derived
    int successes() {
        return (int) stream().filter(PaxosResponse::isSuccessful).entries().count();
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
        return stream().entries().anyMatch(entry -> !entry.getValue().isSuccessful());
    }

    public final PaxosResponses<T> withoutRemotes() {
        return PaxosResponses.of(quorum(), ImmutableList.copyOf(responses().values()));
    }
}
