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
import static java.util.stream.Collectors.toMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosConstants;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponses;
import com.palantir.paxos.PaxosResponsesWithRemote;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.immutables.value.Value;

public class PaxosQuorumCheckingCoalescingFunction<
                REQ, RESP extends PaxosResponse, FUNC extends CoalescingRequestFunction<REQ, RESP>>
        implements CoalescingRequestFunction<REQ, PaxosResponsesWithRemote<FUNC, RESP>> {

    private final List<FUNC> delegates;
    private final Map<FUNC, CheckedRejectionExecutorService> executors;
    private final int quorumSize;
    private final PaxosResponsesWithRemote<FUNC, RESP> defaultValue;

    public PaxosQuorumCheckingCoalescingFunction(
            List<FUNC> delegateFunctions, Map<FUNC, CheckedRejectionExecutorService> executors, int quorumSize) {
        this.delegates = delegateFunctions;
        this.executors = executors;
        this.quorumSize = quorumSize;
        this.defaultValue = PaxosResponsesWithRemote.of(quorumSize, ImmutableMap.of());
    }

    @Override
    public Map<REQ, PaxosResponsesWithRemote<FUNC, RESP>> apply(Set<REQ> requests) {
        PaxosResponsesWithRemote<FUNC, PaxosContainer<Map<REQ, RESP>>> responses =
                PaxosQuorumChecker.collectQuorumResponses(
                        ImmutableList.copyOf(delegates),
                        delegate -> PaxosContainer.of(delegate.apply(requests)),
                        quorumSize,
                        executors,
                        PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                        PaxosConstants.CANCEL_REMAINING_CALLS);

        Map<REQ, PaxosResponsesWithRemote<FUNC, RESP>> responseMap = responses.stream()
                .map(PaxosContainer::get)
                .map(PaxosQuorumCheckingCoalescingFunction::attachFunctionToEntry)
                .values()
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(groupingBy(
                        Map.Entry::getKey,
                        mapping(
                                Map.Entry::getValue,
                                collectingAndThen(
                                        toMap(Map.Entry::getKey, Map.Entry::getValue),
                                        singleResponse -> PaxosResponsesWithRemote.of(quorumSize, singleResponse)))));

        return Maps.toMap(requests, request -> responseMap.getOrDefault(request, defaultValue));
    }

    private static <REQ, RESP, FUNC> Map<REQ, Map.Entry<FUNC, RESP>> attachFunctionToEntry(
            FUNC function, Map<REQ, RESP> map) {
        return KeyedStream.stream(map)
                .map(resp -> Maps.immutableEntry(function, resp))
                .collectToMap();
    }

    public static <REQ, RESP extends PaxosResponse, SERVICE, F extends CoalescingRequestFunction<REQ, RESP>>
            PaxosQuorumCheckingCoalescingFunction<REQ, RESP, F> wrapWithRemotes(
                    List<SERVICE> services,
                    Map<SERVICE, CheckedRejectionExecutorService> executors,
                    int quorumSize,
                    Function<SERVICE, F> functionFactory) {
        List<F> functions = new ArrayList<>(services.size());
        Map<F, CheckedRejectionExecutorService> executorMap = new HashMap<>(services.size());
        for (SERVICE service : services) {
            F function = functionFactory.apply(service);
            functions.add(function);
            executorMap.put(function, executors.get(service));
        }
        return new PaxosQuorumCheckingCoalescingFunction<>(functions, executorMap, quorumSize);
    }

    public static <REQ, RESP extends PaxosResponse, SERVICE, FUNCTION extends CoalescingRequestFunction<REQ, RESP>>
            CoalescingRequestFunction<REQ, PaxosResponses<RESP>> wrap(
                    List<SERVICE> services,
                    Map<SERVICE, CheckedRejectionExecutorService> executorMap,
                    int quorumSize,
                    Function<SERVICE, FUNCTION> functionFactory) {

        PaxosQuorumCheckingCoalescingFunction<REQ, RESP, FUNCTION> wrap =
                wrapWithRemotes(services, executorMap, quorumSize, functionFactory);

        return request -> KeyedStream.stream(wrap.apply(request))
                .map(PaxosResponsesWithRemote::withoutRemotes)
                .collectToMap();
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

    @Value.Immutable
    interface FunctionAndExecutor<F> {
        @Value.Parameter
        F function();

        @Value.Parameter
        CheckedRejectionExecutorService executor();
    }
}
