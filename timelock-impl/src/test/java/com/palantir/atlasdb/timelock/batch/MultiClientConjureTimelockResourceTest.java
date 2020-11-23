/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.tokens.auth.AuthHeader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class MultiClientConjureTimelockResourceTest {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer test");
    private static final int REMOTE_PORT = 4321;
    private static final URL LOCAL = url("https://localhost:1234");
    private static final URL REMOTE = url("https://localhost:" + REMOTE_PORT);
    private static final RedirectRetryTargeter TARGETER =
            RedirectRetryTargeter.create(LOCAL, ImmutableList.of(LOCAL, REMOTE));

    private Map<String, AsyncTimelockService> namespaces = new HashMap();
    private Map<String, LeadershipId> namespaceToLeaderMap = new HashMap();
    private MultiClientConjureTimelockResource resource;

    @Before
    public void before() {
        resource = new MultiClientConjureTimelockResource(TARGETER, this::getServiceForClient);
    }

    @Test
    public void canGetLeaderTimesForMultipleClients() {
        Namespace client1 = Namespace.of("client1");
        Namespace client2 = Namespace.of("client2");
        Set<Namespace> namespaces = ImmutableSet.of(client1, client2);

        LeaderTimes leaderTimesResponse = Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces));
        Map<Namespace, LeaderTime> leaderTimes = leaderTimesResponse.getLeaderTimes();

        // leaderTimes for namespaces are computed by their respective underlying AsyncTimelockService instances
        leaderTimes.forEach((namespace, leaderTime) -> {
            assertThat(leaderTime.id()).isEqualTo(namespaceToLeaderMap.get(namespace.get()));
        });

        // there should be as many leaders as there are distinct clients
        Set<UUID> leaders = leaderTimes.values().stream()
                .map(LeaderTime::id)
                .map(LeadershipId::id)
                .collect(Collectors.toSet());
        assertThat(leaders).hasSameSizeAs(namespaces);
    }

    @Test
    public void requestThrowsIfAnyQueryFails() {
        String throwingClient = "alpha";
        Set<Namespace> namespaces = ImmutableSet.of(Namespace.of(throwingClient), Namespace.of("beta"));
        when(getServiceForClient(throwingClient).leaderTime()).thenThrow(new BlockingTimeoutException(""));
        assertThatThrownBy(() -> Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces)))
                .isInstanceOf(BlockingTimeoutException.class);
    }

    private AsyncTimelockService getServiceForClient(String client) {
        return namespaces.computeIfAbsent(client, this::createAsyncTimeLockServiceForClient);
    }

    private AsyncTimelockService createAsyncTimeLockServiceForClient(String client) {
        AsyncTimelockService timelockService = mock(AsyncTimelockService.class);
        LeadershipId leadershipId = LeadershipId.random();
        namespaceToLeaderMap.put(client, leadershipId);
        when(timelockService.leaderTime())
                .thenReturn(Futures.immediateFuture(LeaderTime.of(leadershipId, NanoTime.createForTests(1L))));
        return timelockService;
    }

    private static URL url(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
