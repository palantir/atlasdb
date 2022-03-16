/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.Test;

public class CassandraAbsentNodeTrackerTest {
    private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(1);
    private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(2);
    private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(3);

    private static final int REQUIRED_CONSECUTIVE_REQUESTS = 3;

    private final CassandraAbsentNodeTracker hostTracker =
            new CassandraAbsentNodeTracker(REQUIRED_CONSECUTIVE_REQUESTS);

    private CassandraClientPoolingContainer container1 = mock(CassandraClientPoolingContainer.class);
    private CassandraClientPoolingContainer container2 = mock(CassandraClientPoolingContainer.class);
    private CassandraClientPoolingContainer container3 = mock(CassandraClientPoolingContainer.class);

    @Test
    public void returnEmptyIfNothingThePool() {
        assertThat(hostTracker.returnPool(ADDRESS_1)).isEmpty();
    }

    @Test
    public void returnPoolIfPresent() {
        hostTracker.trackAbsentHost(ADDRESS_1, container1);
        assertThat(hostTracker.returnPool(ADDRESS_1)).hasValue(container1);
        assertThat(hostTracker.returnPool(ADDRESS_1)).isEmpty();
    }

    @Test
    public void removesServerAfterConsecutiveRequests() {
        hostTracker.trackAbsentHost(ADDRESS_1, container1);
        verifyNoInteractions(container1);

        IntStream.range(0, REQUIRED_CONSECUTIVE_REQUESTS).forEach(_u -> {
            Set<InetSocketAddress> removedHosts = hostTracker.incrementAbsenceAndRemove();
            assertThat(removedHosts).isEmpty();
            verifyNoInteractions(container1);
        });
        Set<InetSocketAddress> removedHosts = hostTracker.incrementAbsenceAndRemove();
        assertThat(removedHosts).containsExactly(ADDRESS_1);
        verifyPoolShutdown(container1);
    }

    @Test
    public void onlyRemoveRelevantHosts() {
        hostTracker.trackAbsentHost(ADDRESS_1, container1);
        hostTracker.trackAbsentHost(ADDRESS_2, container2);

        // increment absence round for addresses 1 and 2.
        hostTracker.incrementAbsenceAndRemove();

        hostTracker.trackAbsentHost(ADDRESS_3, container3);

        IntStream.range(0, REQUIRED_CONSECUTIVE_REQUESTS - 1).forEach(_u -> hostTracker.incrementAbsenceAndRemove());
        Set<InetSocketAddress> removedHosts = hostTracker.incrementAbsenceAndRemove();
        assertThat(removedHosts).containsExactly(ADDRESS_1, ADDRESS_2);
        verifyPoolShutdown(container1);
        verifyPoolShutdown(container2);
        verifyNoInteractions(container3);
    }

    @Test
    public void alwaysRecommendsServersToBeRemovedIfConfiguredWithLimitOne() {
        CassandraAbsentNodeTracker oneShotHostTracker = new CassandraAbsentNodeTracker(0);
        oneShotHostTracker.trackAbsentHost(ADDRESS_1, container1);
        Set<InetSocketAddress> removedHosts = oneShotHostTracker.incrementAbsenceAndRemove();
        assertThat(removedHosts).containsExactly(ADDRESS_1);
        verifyPoolShutdown(container1);
    }

    private void verifyPoolShutdown(CassandraClientPoolingContainer container) {
        verify(container).shutdownPooling();
        verifyNoMoreInteractions(container);
    }
}
