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

import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.Test;

public class CassandraAbsentHostTrackerTest {
    private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(1);
    private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(2);
    private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(3);

    private static final CassandraServer SERVER_1 = CassandraServer.from(ADDRESS_1);
    private static final CassandraServer SERVER_2 = CassandraServer.from(ADDRESS_2);
    private static final CassandraServer SERVER_3 = CassandraServer.from(ADDRESS_3);

    private static final int REQUIRED_CONSECUTIVE_REQUESTS = 3;

    private final CassandraAbsentHostTracker hostTracker =
            new CassandraAbsentHostTracker(REQUIRED_CONSECUTIVE_REQUESTS);

    private CassandraClientPoolingContainer container1 = mock(CassandraClientPoolingContainer.class);
    private CassandraClientPoolingContainer container2 = mock(CassandraClientPoolingContainer.class);
    private CassandraClientPoolingContainer container3 = mock(CassandraClientPoolingContainer.class);

    @Test
    public void returnEmptyIfNothingThePool() {
        assertThat(hostTracker.returnPoolsForCassandraHost(SERVER_1)).isEmpty();
    }

    @Test
    public void returnPoolIfPresent() {
        hostTracker.trackAbsentCassandraServer(SERVER_1, container1);
        assertThat(hostTracker.returnPoolsForCassandraHost(SERVER_1)).hasValue(container1);
        assertThat(hostTracker.returnPoolsForCassandraHost(SERVER_1)).isEmpty();
    }

    @Test
    public void removesServerAfterConsecutiveRequests() {
        hostTracker.trackAbsentCassandraServer(SERVER_1, container1);
        verifyNoInteractions(container1);

        IntStream.range(0, REQUIRED_CONSECUTIVE_REQUESTS).forEach(_u -> {
            Set<CassandraServer> removedHosts = hostTracker.incrementAbsenceAndRemove();
            assertThat(removedHosts).isEmpty();
            verifyNoInteractions(container1);
        });
        Set<CassandraServer> removedHosts = hostTracker.incrementAbsenceAndRemove();
        assertThat(removedHosts).containsExactly(SERVER_1);
        verifyPoolShutdown(container1);
    }

    @Test
    public void onlyRemoveRelevantHosts() {
        hostTracker.trackAbsentCassandraServer(SERVER_1, container1);
        hostTracker.trackAbsentCassandraServer(SERVER_2, container2);

        // increment absence round for addresses 1 and 2.
        hostTracker.incrementAbsenceAndRemove();

        hostTracker.trackAbsentCassandraServer(SERVER_3, container3);

        IntStream.range(0, REQUIRED_CONSECUTIVE_REQUESTS - 1).forEach(_u -> hostTracker.incrementAbsenceAndRemove());
        Set<CassandraServer> removedHosts = hostTracker.incrementAbsenceAndRemove();
        assertThat(removedHosts).containsExactly(SERVER_1, SERVER_2);
        verifyPoolShutdown(container1);
        verifyPoolShutdown(container2);
        verifyNoInteractions(container3);
    }

    @Test
    public void alwaysRecommendsServersToBeRemovedIfConfiguredWithLimitOne() {
        CassandraAbsentHostTracker oneShotHostTracker = new CassandraAbsentHostTracker(0);
        oneShotHostTracker.trackAbsentCassandraServer(SERVER_1, container1);
        Set<CassandraServer> removedHosts = oneShotHostTracker.incrementAbsenceAndRemove();
        assertThat(removedHosts).containsExactly(SERVER_1);
        verifyPoolShutdown(container1);
    }

    private void verifyPoolShutdown(CassandraClientPoolingContainer container) {
        verify(container).shutdownPooling();
        verifyNoMoreInteractions(container);
    }
}
