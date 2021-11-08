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

import static java.util.stream.Collectors.toSet;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.LeaderPingerContext;
import com.palantir.paxos.PaxosConstants;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponsesWithRemote;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;

/*
   This is not thread safe, but it is okay because it is run within an autobatcher, which is configured to not process
   multiple batches in parallel.

   In addition, since this is not a `CoalescingRequestFunction` we must ensure that no internal state is mutated
   inside a Future. The `accept` method is ready to be called again, the moment it returns.
*/
@NotThreadSafe
class GetSuspectedLeaderWithUuid implements Consumer<List<BatchElement<UUID, Optional<ClientAwareLeaderPinger>>>> {

    private static final SafeLogger log = SafeLoggerFactory.get(GetSuspectedLeaderWithUuid.class);

    private final Map<LeaderPingerContext<BatchPingableLeader>, CheckedRejectionExecutorService> executors;
    private final BiMap<LeaderPingerContext<BatchPingableLeader>, ClientAwareLeaderPinger> clientAwareLeaders;
    private final UUID localUuid;
    private final Duration leaderPingResponseWait;

    private final Map<UUID, LeaderPingerContext<BatchPingableLeader>> cache = new HashMap<>();

    GetSuspectedLeaderWithUuid(
            Map<LeaderPingerContext<BatchPingableLeader>, CheckedRejectionExecutorService> executors,
            Set<ClientAwareLeaderPinger> clientAwareLeaderPingers,
            UUID localUuid,
            Duration leaderPingResponseWait) {
        this.executors = executors;
        this.clientAwareLeaders = KeyedStream.of(clientAwareLeaderPingers)
                .mapKeys(ClientAwareLeaderPinger::underlyingRpcClient)
                .collectTo(HashBiMap::create);
        this.localUuid = localUuid;
        this.leaderPingResponseWait = leaderPingResponseWait;
    }

    @Override
    public void accept(List<BatchElement<UUID, Optional<ClientAwareLeaderPinger>>> batchElements) {
        Multimap<UUID, DisruptorFuture<Optional<ClientAwareLeaderPinger>>> uuidsToRequests = batchElements.stream()
                .collect(ImmutableListMultimap.toImmutableListMultimap(BatchElement::argument, BatchElement::result));

        KeyedStream.of(uuidsToRequests.keySet())
                .filterKeys(cache::containsKey)
                .map(cache::get)
                .forEach((cachedUuid, pingable) -> completeRequest(
                        uuidsToRequests.get(cachedUuid), Optional.of(clientAwareLeaders.get(pingable))));

        Set<UUID> uncachedUuids = uuidsToRequests.keySet().stream()
                .filter(uuid -> !cache.containsKey(uuid))
                .collect(toSet());

        if (uncachedUuids.isEmpty()) {
            return;
        }

        PaxosResponsesWithRemote<LeaderPingerContext<BatchPingableLeader>, PaxosContainer<UUID>> results =
                PaxosQuorumChecker.collectUntil(
                        ImmutableList.copyOf(executors.keySet()),
                        pingable -> PaxosContainer.of(pingable.pinger().uuid()),
                        executors,
                        leaderPingResponseWait,
                        state -> state.responses().values().stream()
                                .map(PaxosContainer::get)
                                .collect(toSet())
                                .containsAll(uncachedUuids),
                        PaxosConstants.CANCEL_REMAINING_CALLS);

        for (Map.Entry<LeaderPingerContext<BatchPingableLeader>, PaxosContainer<UUID>> resultEntries :
                results.responses().entrySet()) {
            LeaderPingerContext<BatchPingableLeader> pingable = resultEntries.getKey();
            UUID uuid = resultEntries.getValue().get();

            LeaderPingerContext<BatchPingableLeader> oldCachedEntry = cache.putIfAbsent(uuid, pingable);
            throwIfInvalidSetup(oldCachedEntry, pingable, uuid);
            completeRequest(uuidsToRequests.get(uuid), Optional.of(clientAwareLeaders.get(pingable)));
        }

        Set<UUID> missingUuids = Sets.difference(
                uncachedUuids,
                results.withoutRemotes().stream().map(PaxosContainer::get).collect(toSet()));

        missingUuids.forEach(missingUuid -> completeRequest(uuidsToRequests.get(missingUuid), Optional.empty()));
    }

    private static void completeRequest(
            Collection<DisruptorFuture<Optional<ClientAwareLeaderPinger>>> futures,
            Optional<ClientAwareLeaderPinger> outcome) {
        futures.forEach(result -> result.set(outcome));
    }

    private void throwIfInvalidSetup(
            LeaderPingerContext<BatchPingableLeader> cachedService,
            LeaderPingerContext<BatchPingableLeader> pingedService,
            UUID pingedServiceUuid) {
        if (cachedService == null) {
            return;
        }

        IllegalStateException exception =
                new SafeIllegalStateException("There is a fatal problem with the leadership election configuration! "
                        + "This is probably caused by invalid pref files setting up the cluster "
                        + "(e.g. for lock server look at lock.prefs, leader.prefs, and lock_client.prefs)."
                        + "If the preferences are specified with a host port pair list and localhost index "
                        + "then make sure that the localhost index is correct (e.g. actually the localhost).");

        if (cachedService != pingedService) {
            log.error("Remote potential leaders are claiming to be each other!", exception);
            throw Throwables.rewrap(exception);
        }

        if (pingedServiceUuid.equals(localUuid)) {
            log.error("Remote potential leader is claiming to be you!", exception);
            throw Throwables.rewrap(exception);
        }
    }
}
