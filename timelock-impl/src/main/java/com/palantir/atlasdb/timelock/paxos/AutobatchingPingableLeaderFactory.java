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

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.LeaderPingResult;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPinger;

public class AutobatchingPingableLeaderFactory implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AutobatchingPingableLeaderFactory.class);

    private final Collection<? extends Closeable> closeables;
    private final DisruptorAutobatcher<UUID, Optional<ClientAwarePingableLeader>> uuidToRemoteAutobatcher;
    private final Map<? extends ClientAwarePingableLeader, ExecutorService> executors;
    private final Duration leaderPingResponseWait;

    private AutobatchingPingableLeaderFactory(
            Collection<? extends Closeable> closeables,
            DisruptorAutobatcher<UUID, Optional<ClientAwarePingableLeader>> uuidAutobatcher,
            Map<? extends ClientAwarePingableLeader, ExecutorService> executors,
            Duration leaderPingResponseWait) {
        this.closeables = closeables;
        this.uuidToRemoteAutobatcher = uuidAutobatcher;
        this.executors = executors;
        this.leaderPingResponseWait = leaderPingResponseWait;
    }

    public static AutobatchingPingableLeaderFactory create(
            Map<BatchPingableLeader, ExecutorService> executors,
            Duration leaderPingResponseWait,
            UUID localUuid) {
        Map<ClientAwarePingableLeader, ExecutorService> clientAwarePingableExecutors = KeyedStream.stream(executors)
                .<ClientAwarePingableLeader>mapKeys(ClientAwarePingableLeaderImpl::create)
                .collectToMap();

        DisruptorAutobatcher<UUID, Optional<ClientAwarePingableLeader>> uuidAutobatcher = Autobatchers.independent(
                new GetSuspectedLeaderWithUuid(clientAwarePingableExecutors, localUuid, leaderPingResponseWait))
                .safeLoggablePurpose("batch-paxos-pingable-leader.uuid")
                .build();

        return new AutobatchingPingableLeaderFactory(
                clientAwarePingableExecutors.keySet(),
                uuidAutobatcher,
                clientAwarePingableExecutors,
                leaderPingResponseWait);
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

    private static final class ClientAwarePingableLeaderImpl implements ClientAwarePingableLeader {

        private final DisruptorAutobatcher<Client, LeaderPingResult> pingAutobatcher;
        private final BatchPingableLeader remoteClient;

        ClientAwarePingableLeaderImpl(
                DisruptorAutobatcher<Client, LeaderPingResult> pingAutobatcher,
                BatchPingableLeader remoteClient) {
            this.pingAutobatcher = pingAutobatcher;
            this.remoteClient = remoteClient;
        }

        static ClientAwarePingableLeaderImpl create(BatchPingableLeader remoteClient) {
            DisruptorAutobatcher<Client, LeaderPingResult> pingAutobatcher =
                    Autobatchers.coalescing(new PingCoalescingFunction(remoteClient))
                            .safeLoggablePurpose("batch-pingable-leader.ping")
                            .build();

            return new ClientAwarePingableLeaderImpl(pingAutobatcher, remoteClient);
        }

        @Override
        public LeaderPingResult ping(Client client) {
            try {
                return pingAutobatcher.apply(client).get();
            } catch (InterruptedException | ExecutionException e) {
                throw AutobatcherExecutionExceptions.handleAutobatcherExceptions(e);
            }
        }

        @Override
        public UUID uuid() {
            return remoteClient.uuid();
        }

        @Override
        public void close() {
            pingAutobatcher.close();
        }
    }

    private final class AutobatchingLeaderPinger implements LeaderPinger {

        private final Client client;

        private AutobatchingLeaderPinger(Client client) {
            this.client = client;
        }

        @Override
        public LeaderPingResult pingLeaderWithUuid(UUID uuid) {
            try {
                Optional<ClientAwarePingableLeader> maybePingableLeader = uuidToRemoteAutobatcher.apply(uuid).get();
                if (!maybePingableLeader.isPresent()) {
                    return LeaderPingResults.pingReturnedFalse();
                }

                ClientAwarePingableLeader pingableLeaderWithUuid = maybePingableLeader.get();
                return executors.get(pingableLeaderWithUuid).submit(() -> pingableLeaderWithUuid.ping(client))
                        .get(leaderPingResponseWait.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("received interrupt whilst trying to ping leader",
                        SafeArg.of("client", client));
                Thread.currentThread().interrupt();
                return LeaderPingResults.pingCallFailure(e);
            } catch (ExecutionException e) {
                log.warn("received error whilst trying to ping leader", e.getCause(),
                        SafeArg.of("client", client));
                return LeaderPingResults.pingCallFailure(e.getCause());
            } catch (TimeoutException e) {
                log.warn("timed out whilst trying to ping leader",
                        SafeArg.of("client", client));
                return LeaderPingResults.pingTimedOut();
            }
        }

    }

}
