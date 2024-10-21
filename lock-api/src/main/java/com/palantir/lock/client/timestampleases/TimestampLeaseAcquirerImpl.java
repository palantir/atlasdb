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

package com.palantir.lock.client.timestampleases;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.LeaseGuarantee;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.RequestId;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.atlasdb.timelock.api.TimestampLeaseRequests;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponses;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.lock.client.LeasedLockToken;
import com.palantir.lock.client.TimeLockUnlocker;
import com.palantir.lock.v2.ImmutableTimestampLeaseResult;
import com.palantir.lock.v2.ImmutableTimestampLeaseResults;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimestampLeaseResult;
import com.palantir.lock.v2.TimestampLeaseResults;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.ids.UniqueIds;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.immutables.value.Value;

final class TimestampLeaseAcquirerImpl implements TimestampLeaseAcquirer {
    private static final SafeLogger log = SafeLoggerFactory.get(TimestampLeaseAcquirerImpl.class);

    private final Retryer<RequestAndResponse> retryer = RetryerBuilder.<RequestAndResponse>newBuilder()
            .retryIfResult(requestAndResponse -> !wasRequestFullyFulfilled(requestAndResponse))
            .retryIfException()
            .withStopStrategy(StopStrategies.stopAfterAttempt(3))
            .build();

    private final Namespace namespace;
    private final InternalMultiClientConjureTimelockService service;
    private final TimeLockUnlocker unlocker;

    TimestampLeaseAcquirerImpl(
            Namespace namespace, InternalMultiClientConjureTimelockService service, TimeLockUnlocker unlocker) {
        this.namespace = namespace;
        this.service = service;
        this.unlocker = unlocker;
    }

    @Override
    public TimestampLeaseResults acquireNamedTimestampLeases(Map<TimestampLeaseName, Integer> requests) {
        TimestampLeaseResponses responses = acquireNamedTimestampLeasesWithRetry(requests);

        LockToken lock = createFromLeaseGuarantee(responses.getLeaseGuarantee());

        Map<TimestampLeaseName, TimestampLeaseResult> results = KeyedStream.stream(
                        responses.getTimestampLeaseResponses())
                .map((timestampName, response) -> createTimestampLeaseResult(requests.get(timestampName), response))
                .collectToMap();

        return ImmutableTimestampLeaseResults.of(lock, results);
    }

    @Override
    public void close() {
        unlocker.close();
    }

    private static TimestampLeaseResult createTimestampLeaseResult(int requested, TimestampLeaseResponse response) {
        return ImmutableTimestampLeaseResult.builder()
                .minLeasedTimestamp(response.getMinLeased())
                .freshTimestampsSupplier(ExactTimestampSupplier.create(response.getFreshTimestamps(), requested))
                .build();
    }

    private TimestampLeaseResponses acquireNamedTimestampLeasesWithRetry(Map<TimestampLeaseName, Integer> requests) {
        try {
            return retryer.call(() -> {
                        TimestampLeaseRequests request =
                                TimestampLeaseRequests.of(RequestId.of(UniqueIds.pseudoRandomUuidV4()), requests);

                        return acquireNamedTimestampLeasesInternal(request);
                    })
                    .response();
        } catch (ExecutionException e) {
            log.warn("Unexpected exception. Retryer should throw a retry exception instead.", e);
            throw new SafeRuntimeException(e);
        } catch (RetryException e) {
            log.warn("Exhausted retries to acquire timestamp lease. This should be transient.", e);
            throw new SafeRuntimeException(e);
        }
    }

    private boolean wasRequestFullyFulfilled(RequestAndResponse requestAndResponse) {
        Map<TimestampLeaseName, Integer> requests =
                requestAndResponse.requests().getNumFreshTimestamps();
        Map<TimestampLeaseName, TimestampLeaseResponse> responses =
                requestAndResponse.response().getTimestampLeaseResponses();

        Preconditions.checkArgument(
                requests.keySet().equals(responses.keySet()),
                "Requests and responses must have the same keys",
                SafeArg.of("requests", requests),
                SafeArg.of("responses", responses));

        boolean wasFullyFulfilled = requests.keySet().stream().allMatch(request -> {
            int requestedTimestamps = requests.get(request);
            long returnedTimestamps =
                    responses.get(request).getFreshTimestamps().getCount();
            return returnedTimestamps >= requestedTimestamps;
        });

        if (!wasFullyFulfilled) {
            log.info(
                    "Timestamp lease request was not fully fulfilled. This should happen infrequently.",
                    SafeArg.of("requests", requests),
                    SafeArg.of("responses", responses));
            unlocker.enqueue(Set.of(
                    createFromLeaseGuarantee(requestAndResponse.response().getLeaseGuarantee())));
        }

        return wasFullyFulfilled;
    }

    private RequestAndResponse acquireNamedTimestampLeasesInternal(TimestampLeaseRequests requests) {
        NamespaceTimestampLeaseRequest batchRequest = NamespaceTimestampLeaseRequest.of(List.of(requests));

        Map<Namespace, NamespaceTimestampLeaseResponse> multiNamespaceResponse =
                service.acquireTimestampLeases(Map.of(namespace, batchRequest));

        NamespaceTimestampLeaseResponse batchResponse =
                validateResponseContainsOnlyNamespace(requests, namespace, multiNamespaceResponse);

        TimestampLeaseResponses responses = validateBatchResponseContainsOnlyOneResponse(requests, batchResponse);

        return RequestAndResponse.of(requests, responses);
    }

    private static TimestampLeaseResponses validateBatchResponseContainsOnlyOneResponse(
            TimestampLeaseRequests requests, NamespaceTimestampLeaseResponse batchResponse) {
        if (batchResponse.getAlias().size() != 1) {
            throw new SafeIllegalArgumentException(
                    "Expecting exactly one response",
                    SafeArg.of("requests", requests),
                    SafeArg.of("response", batchResponse));
        }

        return batchResponse.getAlias().get(0);
    }

    private static NamespaceTimestampLeaseResponse validateResponseContainsOnlyNamespace(
            TimestampLeaseRequests requests,
            Namespace namespace,
            Map<Namespace, NamespaceTimestampLeaseResponse> responses) {
        if (responses.size() != 1) {
            throw new SafeIllegalArgumentException(
                    "Expected exactly one response",
                    SafeArg.of("requests", requests),
                    SafeArg.of("responses", responses),
                    SafeArg.of("namespace", namespace));
        }

        return Preconditions.checkNotNull(
                responses.get(namespace),
                "Expected response for namespace",
                SafeArg.of("requests", requests),
                SafeArg.of("responses", responses),
                SafeArg.of("namespace", namespace));
    }

    private static LockToken createFromLeaseGuarantee(LeaseGuarantee leaseGuarantee) {
        return LeasedLockToken.of(
                ConjureLockToken.of(leaseGuarantee.getIdentifier().get()), leaseGuarantee.getLease());
    }

    @Value.Immutable
    interface RequestAndResponse {
        @Value.Parameter
        TimestampLeaseRequests requests();

        @Value.Parameter
        TimestampLeaseResponses response();

        static RequestAndResponse of(TimestampLeaseRequests requests, TimestampLeaseResponses response) {
            return ImmutableRequestAndResponse.of(requests, response);
        }
    }
}
