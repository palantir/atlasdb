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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.atlasdb.timelock.api.LeaseGuarantee;
import com.palantir.atlasdb.timelock.api.LeaseIdentifier;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.RequestId;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.atlasdb.timelock.api.TimestampLeaseRequests;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponses;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.client.LeasedLockToken;
import com.palantir.lock.client.timestampleases.TimestampLeaseAcquirerImpl.Unlocker;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.TimestampLeaseResults;
import com.palantir.logsafe.SafeArg;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;

@ExtendWith(MockitoExtension.class)
public final class TimestampLeaseAcquirerImplTest {
    private static final TimestampLeaseName NAME_1 = TimestampLeaseName.of("name1");
    private static final TimestampLeaseName NAME_2 = TimestampLeaseName.of("name2");

    @Mock
    private NamespacedTimestampLeaseService service;

    @Mock
    private Unlocker unlocker;

    @Mock
    private Supplier<UUID> uuidSupplier;

    private TimestampLeaseAcquirer acquirer;

    @BeforeEach
    public void before() {
        acquirer = new TimestampLeaseAcquirerImpl(service, unlocker, uuidSupplier);
    }

    @Test
    public void returnsLeasedLockTokenWithMatchingLeaseAndServerTokenWhenNoRetryWithNoCleanup() {
        UUID uuid = UUID.randomUUID();
        Lease lease = createRandomLease();
        Map<TimestampLeaseName, Integer> requested = Map.of(NAME_1, 10, NAME_2, 5);

        when(uuidSupplier.get()).thenReturn(uuid).thenThrow(new RuntimeException("not expected to be invoked again"));
        when(service.acquireTimestampLeases(createRequest(requested, uuid)))
                .thenReturn(createResponseWithEnoughTimestamps(requested, uuid, lease));

        TimestampLeaseResults results = acquirer.acquireNamedTimestampLeases(requested);

        assertThat(results.lock()).isInstanceOf(LeasedLockToken.class);
        LeasedLockToken asLeasedLockToken = (LeasedLockToken) results.lock();
        assertThat(asLeasedLockToken.serverToken()).isEqualTo(ConjureLockToken.of(uuid));
        assertThat(asLeasedLockToken.getLease()).isEqualTo(lease);
        verifyNoInteractions(unlocker);
    }

    @ValueSource(ints = {1, 2})
    @ParameterizedTest
    public void succeedsWhenTimeLockOnlyReturnsEnoughTimestampsOnRetryAndCleansUpLocks(int numFailedRetries) {
        List<UUID> uuidsForFailedRequests =
                Stream.generate(UUID::randomUUID).limit(numFailedRetries).collect(Collectors.toList());
        UUID uuidForSuccessfulRequest = UUID.randomUUID();
        Lease leaseForSuccessfulRequest = createRandomLease();
        Map<TimestampLeaseName, Integer> requested = Map.of(NAME_1, 10, NAME_2, 5);

        stubUuidSupplier(uuidsForFailedRequests, uuidForSuccessfulRequest);
        for (UUID uuidForFailedRequest : uuidsForFailedRequests) {
            when(service.acquireTimestampLeases(createRequest(requested, uuidForFailedRequest)))
                    .thenReturn(createResponseWithoutEnoughTimestamps(requested, uuidForFailedRequest));
        }
        when(service.acquireTimestampLeases(createRequest(requested, uuidForSuccessfulRequest)))
                .thenReturn(createResponseWithEnoughTimestamps(
                        requested, uuidForSuccessfulRequest, leaseForSuccessfulRequest));

        TimestampLeaseResults results = acquirer.acquireNamedTimestampLeases(requested);

        assertThat(results.lock()).isInstanceOf(LeasedLockToken.class);
        LeasedLockToken asLeasedLockToken = (LeasedLockToken) results.lock();
        assertThat(asLeasedLockToken.serverToken()).isEqualTo(ConjureLockToken.of(uuidForSuccessfulRequest));
        assertThat(asLeasedLockToken.getLease()).isEqualTo(leaseForSuccessfulRequest);

        for (UUID uuidForFailedRequest : uuidsForFailedRequests) {
            verify(unlocker).unlock(LeaseIdentifier.of(uuidForFailedRequest));
        }
        verifyNoMoreInteractions(unlocker);
    }

    @Test
    public void throwsAtlasDbDependencyExceptionWhenRetriesAreExhausted() {
        int maxRetries = 3;
        List<UUID> uuids = Stream.generate(UUID::randomUUID).limit(maxRetries).collect(Collectors.toList());
        Map<TimestampLeaseName, Integer> requested = Map.of(NAME_1, 10, NAME_2, 5);

        stubUuidSupplier(uuids);
        for (UUID uuidForFailedRequest : uuids) {
            when(service.acquireTimestampLeases(createRequest(requested, uuidForFailedRequest)))
                    .thenReturn(createResponseWithoutEnoughTimestamps(requested, uuidForFailedRequest));
        }

        assertThatLoggableExceptionThrownBy(() -> acquirer.acquireNamedTimestampLeases(requested))
                .isExactlyInstanceOf(AtlasDbDependencyException.class)
                .containsArgs(SafeArg.of("requests", requested), SafeArg.of("numRetries", 3L));

        for (UUID uuid : uuids) {
            verify(unlocker).unlock(LeaseIdentifier.of(uuid));
        }
        verifyNoMoreInteractions(unlocker);
    }

    private void stubUuidSupplier(List<UUID> uuids, UUID... moreUuids) {
        OngoingStubbing<UUID> stubbing = when(uuidSupplier.get());
        for (UUID uuid : uuids) {
            stubbing = stubbing.thenReturn(uuid);
        }
        for (UUID uuid : moreUuids) {
            stubbing = stubbing.thenReturn(uuid);
        }
        stubbing.thenThrow(new RuntimeException("not expected to be invoked again"));
    }

    private static NamespaceTimestampLeaseRequest createRequest(Map<TimestampLeaseName, Integer> requested, UUID uuid) {
        return NamespaceTimestampLeaseRequest.of(List.of(TimestampLeaseRequests.of(RequestId.of(uuid), requested)));
    }

    private static NamespaceTimestampLeaseResponse createResponseWithoutEnoughTimestamps(
            Map<TimestampLeaseName, Integer> requestedMap, UUID uuid) {
        LeaseGuarantee leaseGuarantee = LeaseGuarantee.of(LeaseIdentifier.of(uuid), createRandomLease());
        int randomIndex = ThreadLocalRandom.current().nextInt(1, requestedMap.size());
        TimestampLeaseName selectedNameForNotEnoughTimestamps =
                requestedMap.keySet().stream().skip(randomIndex - 1).findFirst().orElseThrow();

        Map<TimestampLeaseName, TimestampLeaseResponse> responses = KeyedStream.stream(requestedMap)
                .map((name, requested) -> {
                    int toFulfill = name.equals(selectedNameForNotEnoughTimestamps) ? requested - 1 : requested;
                    return TimestampLeaseResponse.of(1, ConjureTimestampRange.of(1, toFulfill));
                })
                .collectToMap();

        return NamespaceTimestampLeaseResponse.of(List.of(TimestampLeaseResponses.of(leaseGuarantee, responses)));
    }

    private static NamespaceTimestampLeaseResponse createResponseWithEnoughTimestamps(
            Map<TimestampLeaseName, Integer> requestedMap, UUID uuid, Lease lease) {
        return NamespaceTimestampLeaseResponse.of(List.of(TimestampLeaseResponses.of(
                LeaseGuarantee.of(LeaseIdentifier.of(uuid), lease),
                KeyedStream.stream(requestedMap)
                        .map(requested -> TimestampLeaseResponse.of(1, ConjureTimestampRange.of(1, requested)))
                        .collectToMap())));
    }

    private static Lease createRandomLease() {
        return Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.now()), Duration.ofMinutes(1));
    }
}
