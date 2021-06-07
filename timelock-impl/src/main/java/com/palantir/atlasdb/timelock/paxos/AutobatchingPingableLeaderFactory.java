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

import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.annotations.ImmutablesStyles.AllParametersStyle;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.Client;
import com.palantir.paxos.LeaderPingResult;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.LeaderPingerContext;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AutobatchingPingableLeaderFactory implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AutobatchingPingableLeaderFactory.class);

    private final Collection<? extends Closeable> closeables;
    private final DisruptorAutobatcher<UUID, Optional<ClientAwareLeaderPinger>> uuidToRemoteAutobatcher;
    private final Duration leaderPingResponseWait;

    private AutobatchingPingableLeaderFactory(
            Collection<? extends Closeable> closeables,
            DisruptorAutobatcher<UUID, Optional<ClientAwareLeaderPinger>> uuidAutobatcher,
            Duration leaderPingResponseWait) {
        this.closeables = closeables;
        this.uuidToRemoteAutobatcher = uuidAutobatcher;
        this.leaderPingResponseWait = leaderPingResponseWait;
    }

    public static AutobatchingPingableLeaderFactory create(
            Map<LeaderPingerContext<BatchPingableLeader>, CheckedRejectionExecutorService> executors,
            Duration pingRate,
            Duration pingResponseWait,
            UUID localUuid) {
        Set<ClientAwareLeaderPinger> clientAwarePingables = executors.keySet().stream()
                .map(remoteClient -> clientAwarePingableLeader(remoteClient, pingRate, pingResponseWait, localUuid))
                .collect(Collectors.toSet());

        DisruptorAutobatcher<UUID, Optional<ClientAwareLeaderPinger>> uuidAutobatcher = Autobatchers.independent(
                        new GetSuspectedLeaderWithUuid(executors, clientAwarePingables, localUuid, pingResponseWait))
                .safeLoggablePurpose("batch-paxos-pingable-leader.uuid")
                .build();

        return new AutobatchingPingableLeaderFactory(clientAwarePingables, uuidAutobatcher, pingResponseWait);
    }

    @Override
    public void close() {
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                log.error("could not close autobatcher for pingable leader");
            }
        }
        uuidToRemoteAutobatcher.close();
    }

    public LeaderPinger leaderPingerFor(Client client) {
        return new AutobatchingLeaderPinger(client);
    }

    private static ClientAwareLeaderPinger clientAwarePingableLeader(
            LeaderPingerContext<BatchPingableLeader> remoteClient,
            Duration leaderPingRate,
            Duration leaderPingResponseWait,
            UUID localUuid) {
        CumulativeLeaderPinger manualBatchingPingableLeader =
                new CumulativeLeaderPinger(remoteClient, leaderPingRate, leaderPingResponseWait, localUuid);
        manualBatchingPingableLeader.startAsync();
        return manualBatchingPingableLeader;
    }

    private final class AutobatchingLeaderPinger implements LeaderPinger {

        private final Client client;

        private AutobatchingLeaderPinger(Client client) {
            this.client = client;
        }

        @Override
        public LeaderPingResult pingLeaderWithUuid(UUID uuid) {
            try {
                Optional<ClientAwareLeaderPinger> maybePingableLeader =
                        uuidToRemoteAutobatcher.apply(uuid).get();
                if (!maybePingableLeader.isPresent()) {
                    return LeaderPingResults.pingReturnedFalse();
                }

                return maybePingableLeader
                        .get()
                        .registerAndPing(uuid, client)
                        .get(leaderPingResponseWait.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("received interrupt whilst trying to ping leader", SafeArg.of("client", client), e);
                Thread.currentThread().interrupt();
                return LeaderPingResults.pingCallFailure(e);
            } catch (ExecutionException e) {
                log.warn("received error whilst trying to ping leader", SafeArg.of("client", client), e.getCause());
                return LeaderPingResults.pingCallFailure(e.getCause());
            } catch (TimeoutException e) {
                log.warn("timed out whilst trying to ping leader", SafeArg.of("client", client), e);
                return LeaderPingResults.pingTimedOut();
            }
        }
    }

    @Value.Immutable
    @AllParametersStyle
    interface PingRequest {
        Client client();

        UUID requestedLeaderId();
    }
}
