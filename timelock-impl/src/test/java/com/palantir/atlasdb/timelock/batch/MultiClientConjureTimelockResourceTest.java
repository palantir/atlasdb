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
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
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

    private AsyncTimelockService timelockService = mock(AsyncTimelockService.class);
    private LeaderTime leaderTime = mock(LeaderTime.class);
    private MultiClientConjureTimelockResource resource;

    @Before
    public void before() {
        resource = new MultiClientConjureTimelockResource(TARGETER, unused -> timelockService);
    }

    @Test
    public void canGetLeaderTimesForMultipleClients() {
        when(timelockService.leaderTime()).thenReturn(Futures.immediateFuture(leaderTime));
        Set<Namespace> namespaces = ImmutableSet.of(Namespace.of("client1"), Namespace.of("client2"));
        assertThat(Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces)))
                .isEqualTo(getLeaderTimesForNamespaces(namespaces));
    }

    @Test
    public void requestThrowsIfAnyQueryFails() {
        when(timelockService.leaderTime())
                .thenReturn(Futures.immediateFuture(leaderTime))
                .thenThrow(new BlockingTimeoutException(""));
        Set<Namespace> namespaces = ImmutableSet.of(Namespace.of("client1"), Namespace.of("client2"));
        assertThatThrownBy(() -> Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces)))
                .isInstanceOf(BlockingTimeoutException.class);
    }

    private LeaderTimes getLeaderTimesForNamespaces(Set<Namespace> namespaces) {
        Map<Namespace, LeaderTime> collect = namespaces.stream().collect(Collectors.toMap(x -> x, x -> leaderTime));
        return LeaderTimes.of(collect);
    }

    private static URL url(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
