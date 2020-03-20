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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.autobatch.AsyncCoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.paxos.PaxosExecutionEnvironment;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponses;

public class PaxosQuorumCheckingCoalescingFunction<REQ, RESP extends PaxosResponse> implements
        CoalescingRequestFunction<REQ, PaxosResponses<RESP>> {

    private final Runner<REQ, RESP> runner;
    private final int quorumSize;
    private final PaxosResponses<RESP> defaultValue;

    public PaxosQuorumCheckingCoalescingFunction(Runner<REQ, RESP> runner, int quorumSize) {
        this.runner = runner;
        this.quorumSize = quorumSize;
        this.defaultValue = PaxosResponses.of(quorumSize, ImmutableList.of());
    }

    @Override
    public Map<REQ, PaxosResponses<RESP>> apply(Set<REQ> requests) {
        PaxosResponses<PaxosContainer<Map<REQ, RESP>>> responses = runner.execute(requests);

        Map<REQ, PaxosResponses<RESP>> responseMap = responses.stream()
                .map(PaxosContainer::get)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(groupingBy(
                        Map.Entry::getKey,
                        mapping(Map.Entry::getValue, collectingAndThen(
                                toList(),
                                singleResponse -> PaxosResponses.of(quorumSize, singleResponse)))));

        return Maps.toMap(requests, request -> responseMap.getOrDefault(request, defaultValue));
    }

    public static <REQ, RESP extends PaxosResponse, SERVICE, F extends CoalescingRequestFunction<REQ, RESP>>
    PaxosQuorumCheckingCoalescingFunction<REQ, RESP> wrapSync(
            PaxosExecutionEnvironment<SERVICE> executionEnvironment,
            int quorumSize,
            Function<SERVICE, F> functionFactory) {
        PaxosExecutionEnvironment<F> mappedExecutionEnvironment = executionEnvironment.map(functionFactory);
        return new PaxosQuorumCheckingCoalescingFunction<>(
                requests -> PaxosQuorumChecker.collectQuorumResponses(
                        mappedExecutionEnvironment,
                        delegate -> PaxosContainer.of(delegate.apply(requests)),
                        quorumSize,
                        PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                        PaxosTimeLockConstants.CANCEL_REMAINING_CALLS).withoutRemotes(),
                quorumSize);
    }

    public static <REQ, RESP extends PaxosResponse, SERVICE, F extends AsyncCoalescingRequestFunction<REQ, RESP>>
    PaxosQuorumCheckingCoalescingFunction<REQ, RESP> wrapAsync(
            PaxosExecutionEnvironment<SERVICE> executionEnvironment,
            int quorumSize,
            Function<SERVICE, F> functionFactory) {
        PaxosExecutionEnvironment<F> mappedExecutionEnvironment = executionEnvironment.map(functionFactory);
        return new PaxosQuorumCheckingCoalescingFunction<>(
                requests -> PaxosQuorumChecker.collectQuorumResponsesAsync(
                        mappedExecutionEnvironment,
                        delegate -> Futures.transform(
                                delegate.apply(requests),
                                PaxosContainer::of,
                                MoreExecutors.directExecutor()),
                        quorumSize,
                        PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                        PaxosTimeLockConstants.CANCEL_REMAINING_CALLS).withoutRemotes(),
                quorumSize);
    }

    private interface Runner<REQ, RESP extends PaxosResponse> {
        PaxosResponses<PaxosContainer<Map<REQ, RESP>>> execute(Set<REQ> requests);
    }

    @Value.Immutable
    public interface PaxosContainer<T> extends PaxosResponse {
        @Value.Parameter
        T get();

        @Override
        default boolean isSuccessful() {
            return true;
        }

        static <T> PaxosContainer<T> of(T contents) {
            return ImmutablePaxosContainer.of(contents);
        }
    }

}
