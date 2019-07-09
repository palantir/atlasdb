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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponses;

public class PaxosQuorumCheckingCoalescingFunction<REQUEST, RESPONSE extends PaxosResponse> implements
        CoalescingRequestFunction<REQUEST, PaxosResponses<RESPONSE>> {

    private final List<CoalescingRequestFunction<REQUEST, RESPONSE>> delegates;
    private final Map<CoalescingRequestFunction<REQUEST, RESPONSE>, ExecutorService> executors;
    private final int quorumSize;

    public PaxosQuorumCheckingCoalescingFunction(
            List<CoalescingRequestFunction<REQUEST, RESPONSE>> delegates,
            Map<CoalescingRequestFunction<REQUEST, RESPONSE>, ExecutorService> executors,
            int quorumSize) {
        this.delegates = delegates;
        this.executors = executors;
        this.quorumSize = quorumSize;
    }

    @Override
    public PaxosResponses<RESPONSE> defaultValue() {
        return PaxosResponses.of(quorumSize, ImmutableList.of());
    }

    @Override
    public Map<REQUEST, PaxosResponses<RESPONSE>> apply(Set<REQUEST> request) {
        PaxosResponses<PaxosContainer<Map<REQUEST, RESPONSE>>> responses = PaxosQuorumChecker.collectQuorumResponses(
                ImmutableList.copyOf(delegates),
                delegate -> new PaxosContainer<>(delegate.apply(request)),
                quorumSize,
                executors,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT);

        return responses.stream()
                .map(PaxosContainer::get)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(groupingBy(
                        Map.Entry::getKey,
                        mapping(Map.Entry::getValue, collectingAndThen(
                                toList(),
                                responseForSingleRequest -> PaxosResponses.of(quorumSize, responseForSingleRequest)))));
    }

    private class PaxosContainer<T> implements PaxosResponse {

        private final T response;

        PaxosContainer(T response) {
            this.response = response;
        }

        @Override
        public boolean isSuccessful() {
            return true;
        }

        public T get() {
            return response;
        }
    }

}
