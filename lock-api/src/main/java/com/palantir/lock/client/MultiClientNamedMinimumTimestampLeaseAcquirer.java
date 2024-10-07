/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.AcquireNamedMinimumTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.AcquireNamedMinimumTimestampLeaseRequests;
import com.palantir.atlasdb.timelock.api.AcquireNamedMinimumTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.AcquireNamedMinimumTimestampLeaseResponses;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.atlasdb.timelock.api.MultiClientAcquireNamedTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.MultiClientAcquireNamedTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.NamedMinimumTimestampLessor;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tritium.ids.UniqueIds;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.immutables.value.Value;

// Batching this means that even if a user submits acquire requests serially from the same threads
// there results might not have increasing timestamps from one call to the other
// I need to document the behaviour in the API
// For our use case, we care about the fact that the timestamps are > than the lock at the time
// And, that the lock stays valid
public final class MultiClientNamedMinimumTimestampLeaseAcquirer implements AutoCloseable {
    private final DisruptorAutobatcher<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse> autobatcher;

    private MultiClientNamedMinimumTimestampLeaseAcquirer(
            DisruptorAutobatcher<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static MultiClientNamedMinimumTimestampLeaseAcquirer create(
            InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse> autobatcher =
                Autobatchers.independent(new AcquireNamedMinimumTimestampLeaseConsumer(delegate))
                        .safeLoggablePurpose("multi-client-acquire-named-timestamp-lease")
                        .batchFunctionTimeout(Duration.ofSeconds(30))
                        .build();
        return new MultiClientNamedMinimumTimestampLeaseAcquirer(autobatcher);
    }

    public AcquireNamedMinimumTimestampLeaseResponse acquireNamedTimestampLease(
            Namespace namespace, String timestampName, long numFreshTimestamps) {
        return AtlasFutures.getUnchecked(autobatcher.apply(new NamespacedRequest() {
            @Override
            public Namespace namespace() {
                return namespace;
            }

            @Override
            public String timestampName() {
                return timestampName;
            }

            @Override
            public long numFreshTimestamps() {
                return numFreshTimestamps;
            }
        }));
    }

    @Override
    public void close() throws Exception {
        autobatcher.close();
    }

    // this probably looks better with batch managers like the other batchers
    // but, was easier for me starting with this version
    private static final class AcquireNamedMinimumTimestampLeaseConsumer
            implements Consumer<List<BatchElement<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse>>> {
        private final InternalMultiClientConjureTimelockService timelockService;

        private AcquireNamedMinimumTimestampLeaseConsumer(InternalMultiClientConjureTimelockService timelockService) {
            this.timelockService = timelockService;
        }

        @Override
        public void accept(
                List<BatchElement<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse>> batchElements) {
            List<NamespacedRequest> requests =
                    batchElements.stream().map(BatchElement::argument).collect(Collectors.toList());

            MultiClientAcquireNamedTimestampLeaseRequest request = createBatchedRequest(requests);

            MultiClientAcquireNamedTimestampLeaseResponse response;
            try {
                response = timelockService.acquireNamedTimestampLease(request);
            } catch (RuntimeException | Error e) {
                batchElements.forEach(element -> element.result().setException(e));
                return;
            }
            fulfillRequests(batchElements, response);
        }

        private static void fulfillRequests(
                List<BatchElement<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse>> batchElements,
                MultiClientAcquireNamedTimestampLeaseResponse response) {
            Map<Namespace, List<BatchElement<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse>>>
                    elementsByNamespace = batchElements.stream()
                            .collect(Collectors.groupingBy(
                                    element -> element.argument().namespace()));

            Map<Namespace, AcquireNamedMinimumTimestampLeaseResponses> responsesByNamespace = response.get();

            elementsByNamespace.forEach((namespace, elements) -> {
                if (!responsesByNamespace.containsKey(namespace)) {
                    // lets just enforce this on the server side
                    throw new SafeIllegalArgumentException(
                            "Missing response for namespace", SafeArg.of("namespace", namespace));
                }
                fulfillRequestsForSingleNamespace(namespace, elements, responsesByNamespace.get(namespace));
            });
        }

        private static void fulfillRequestsForSingleNamespace(
                Namespace namespace,
                List<BatchElement<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse>> batchElements,
                AcquireNamedMinimumTimestampLeaseResponses responses) {
            Map<
                            NamedMinimumTimestampLessor,
                            List<BatchElement<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse>>>
                    elementsByLessor = batchElements.stream()
                            .collect(Collectors.groupingBy(element -> NamedMinimumTimestampLessor.of(
                                    element.argument().timestampName())));

            Map<NamedMinimumTimestampLessor, AcquireNamedMinimumTimestampLeaseResponse> responsesByLessor =
                    responses.get();

            elementsByLessor.forEach((lessor, elements) -> {
                if (!responsesByLessor.containsKey(lessor)) {
                    // lets just enforce this on the server side
                    // this is bad unexpected state - no need to set the other futures to an exception
                    // or no?
                    throw new SafeIllegalArgumentException("Missing response for lessor", SafeArg.of("lessor", lessor));
                }
                fulfillRequestsForSingleLessor(elements, responsesByLessor.get(lessor));
            });
        }

        private static void fulfillRequestsForSingleLessor(
                List<BatchElement<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse>> elements,
                AcquireNamedMinimumTimestampLeaseResponse response) {
            ConjureTimestampRange freshTimestamps = response.getFreshTimestamps();

            long alreadyGiven = 0;
            for (BatchElement<NamespacedRequest, AcquireNamedMinimumTimestampLeaseResponse> batchElement : elements) {
                long requested = batchElement.argument().numFreshTimestamps();
                long remaining = freshTimestamps.getCount() - alreadyGiven;
                if (requested > remaining) {
                    // lets just enforce this on the server side
                    // this is bad unexpected state - no need to set the other futures to an exception
                    // or no?
                    throw new SafeIllegalStateException("Bad state");
                }
                ConjureTimestampRange responseRange = ConjureTimestampRange.builder()
                        .start(freshTimestamps.getStart() + alreadyGiven)
                        .count(requested)
                        .build();
                alreadyGiven += requested;
                batchElement
                        .result()
                        .set(AcquireNamedMinimumTimestampLeaseResponse.of(
                                response.getMinimumLeasedTimestamp(), response.getLeaseToken(), responseRange));
            }
        }

        private static MultiClientAcquireNamedTimestampLeaseRequest createBatchedRequest(
                List<NamespacedRequest> requests) {
            Map<Namespace, Map<NamedMinimumTimestampLessor, Long>> byNamespaceAndLessor =
                    accumulateRequestedFreshTimestampCountsByNamespaceAndLessor(requests);

            Map<Namespace, AcquireNamedMinimumTimestampLeaseRequests> requestMap = KeyedStream.stream(
                            byNamespaceAndLessor)
                    .map(AcquireNamedMinimumTimestampLeaseConsumer::createSingleNamespaceRequestsObject)
                    .collectToMap();

            return MultiClientAcquireNamedTimestampLeaseRequest.of(requestMap);
        }

        private static AcquireNamedMinimumTimestampLeaseRequests createSingleNamespaceRequestsObject(
                Map<NamedMinimumTimestampLessor, Long> singleNamespaceRequestedFreshTimestampPerLessor) {
            // We want a single request ID per namespace, lessor. This is because the id is what identified the lock
            // token
            // And, the plan is to have all the requests for one namespace, lessor pair to share the lock
            Map<NamedMinimumTimestampLessor, AcquireNamedMinimumTimestampLeaseRequest> requestMap = KeyedStream.stream(
                            singleNamespaceRequestedFreshTimestampPerLessor)
                    .map(numFreshTimestamps -> AcquireNamedMinimumTimestampLeaseRequest.of(
                            // the request should let you return a long!
                            UniqueIds.pseudoRandomUuidV4(), numFreshTimestamps.intValue()))
                    .collectToMap();

            return AcquireNamedMinimumTimestampLeaseRequests.of(requestMap);
        }

        private static Map<Namespace, Map<NamedMinimumTimestampLessor, Long>>
                accumulateRequestedFreshTimestampCountsByNamespaceAndLessor(List<NamespacedRequest> requests) {
            // We should probably switch the API to use a long everywhere
            // Realistically, no one should be asking for more fresh timestamps (even if we aggregate)
            // Using a long might help, or we could do a sum operation that throws on overflow
            Map<Namespace, Map<NamedMinimumTimestampLessor, Long>> numFreshTimestampsRequestMap = new HashMap<>();
            for (NamespacedRequest request : requests) {
                Map<NamedMinimumTimestampLessor, Long> forLessor =
                        numFreshTimestampsRequestMap.computeIfAbsent(request.namespace(), _ignored -> new HashMap<>());
                NamedMinimumTimestampLessor lessor = NamedMinimumTimestampLessor.of(request.timestampName());
                forLessor.put(lessor, forLessor.getOrDefault(lessor, 0L) + request.numFreshTimestamps());
            }
            return numFreshTimestampsRequestMap;
        }
    }

    @Value.Immutable
    interface NamespacedRequest {
        Namespace namespace();

        String timestampName();

        long numFreshTimestamps();
    }
}
