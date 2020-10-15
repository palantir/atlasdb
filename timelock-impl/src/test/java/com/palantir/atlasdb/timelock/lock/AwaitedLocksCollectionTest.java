/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.Test;

public class AwaitedLocksCollectionTest {

    private static final UUID REQUEST_1 = UUID.randomUUID();
    private static final UUID REQUEST_2 = UUID.randomUUID();

    private AwaitedLocksCollection awaitedLocks = new AwaitedLocksCollection();

    @Test
    public void callsSupplierForNewRequests() {
        Supplier<AsyncResult<Void>> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(new AsyncResult<>());
        awaitedLocks.getExistingOrAwait(REQUEST_1, supplier);
        awaitedLocks.getExistingOrAwait(REQUEST_2, supplier);

        verify(supplier, times(2)).get();
    }

    @Test
    public void doesNotCallSupplierForExistingRequest() {
        awaitedLocks.getExistingOrAwait(REQUEST_1, () -> new AsyncResult<>());

        Supplier<AsyncResult<Void>> supplier = mock(Supplier.class);
        awaitedLocks.getExistingOrAwait(REQUEST_1, supplier);

        verifyNoMoreInteractions(supplier);
    }

    @Test
    public void removesRequestWhenComplete() {
        AsyncResult<Void> result = new AsyncResult<>();
        awaitedLocks.getExistingOrAwait(REQUEST_1, () -> result);

        assertThat(awaitedLocks.requestsById).containsKey(REQUEST_1);

        result.complete(null);
        assertRequestsWereRemoved(REQUEST_1);
    }

    @Test
    public void removesRequestIfCompleteSynchronously() {
        AsyncResult<Void> result = AsyncResult.completedResult();
        awaitedLocks.getExistingOrAwait(REQUEST_1, () -> result);

        assertRequestsWereRemoved(REQUEST_1);
    }

    @Test
    public void removesRequestIfSupplierThrows() {
        assertThatThrownBy(() ->
                awaitedLocks.getExistingOrAwait(REQUEST_1, () -> {
                    throw new RuntimeException("foo");
                })).isInstanceOf(RuntimeException.class);

        assertRequestsWereRemoved(REQUEST_1);
    }

    @Test
    public void removesRequestWhenFailedOrTimesOut() {
        AsyncResult<Void> result1 = new AsyncResult<>();
        AsyncResult<Void> result2 = new AsyncResult<>();
        awaitedLocks.getExistingOrAwait(REQUEST_1, () -> result1);
        awaitedLocks.getExistingOrAwait(REQUEST_1, () -> result2);

        result1.fail(new RuntimeException("test"));
        result2.timeout();

        assertRequestsWereRemoved(REQUEST_1, REQUEST_2);
    }

    private void assertRequestsWereRemoved(UUID... requests) {
        Awaitility.await()
                .atMost(Duration.ONE_SECOND)
                .pollInterval(5, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    for (UUID requestId : Arrays.asList(requests)) {
                        assertThat(awaitedLocks.requestsById).doesNotContainKey(requestId);
                    }
                });
    }

}
