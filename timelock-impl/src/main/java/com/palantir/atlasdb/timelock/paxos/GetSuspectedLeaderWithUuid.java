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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.common.base.Throwables;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponsesWithRemote;

/*
    This is not thread safe, but it is okay because it is run within an autobatcher, which is configured to not process
    multiple batches in parallel.

    In addition, since this is not a `CoalescingRequestFunction` we must ensure that no internal state is mutated
    inside a Future. The `accept` method is ready to be called again, the moment it returns.
 */
@NotThreadSafe
class GetSuspectedLeaderWithUuid implements Consumer<List<BatchElement<UUID, Optional<ClientAwarePingableLeader>>>> {

    private static final Logger log = LoggerFactory.getLogger(GetSuspectedLeaderWithUuid.class);

    private final Map<ClientAwarePingableLeader, ExecutorService> executors;
    private final UUID localUuid;
    private final Duration leaderPingResponseWait;

    private final Map<UUID, ClientAwarePingableLeader> cache = Maps.newHashMap();

    GetSuspectedLeaderWithUuid(
            Map<ClientAwarePingableLeader, ExecutorService> executors,
            UUID localUuid,
            Duration leaderPingResponseWait) {
        this.executors = executors;
        this.localUuid = localUuid;
        this.leaderPingResponseWait = leaderPingResponseWait;
    }

    @Override
    public void accept(List<BatchElement<UUID, Optional<ClientAwarePingableLeader>>> batchElements) {
        Multimap<UUID, SettableFuture<Optional<ClientAwarePingableLeader>>> uuidsToRequests = batchElements.stream()
                .collect(ImmutableListMultimap.toImmutableListMultimap(BatchElement::argument, BatchElement::result));

        KeyedStream.of(uuidsToRequests.keySet())
                .filterKeys(cache::containsKey)
                .map(cache::get)
                .forEach((cachedUuid, pingable) -> completeRequest(uuidsToRequests, cachedUuid, Optional.of(pingable)));

        Set<UUID> uncachedUuids = uuidsToRequests.keySet().stream()
                .filter(uuid -> !cache.containsKey(uuid))
                .collect(toSet());

        if (uncachedUuids.isEmpty()) {
            return;
        }

        PaxosResponsesWithRemote<ClientAwarePingableLeader, PaxosContainer<UUID>> results =
                PaxosQuorumChecker.collectUntil(
                ImmutableList.copyOf(executors.keySet()),
                pingable -> PaxosContainer.of(pingable.uuid()),
                executors,
                leaderPingResponseWait,
                state -> state.responses().values().stream().map(PaxosContainer::get).collect(toSet())
                        .containsAll(uncachedUuids));

        for (Map.Entry<ClientAwarePingableLeader, PaxosContainer<UUID>> resultEntries : results.responses().entrySet()) {
            ClientAwarePingableLeader pingable = resultEntries.getKey();
            UUID uuid = resultEntries.getValue().get();

            ClientAwarePingableLeader oldCachedEntry = cache.putIfAbsent(uuid, pingable);
            throwIfInvalidSetup(oldCachedEntry, pingable, uuid);
            completeRequest(uuidsToRequests, uuid, Optional.of(pingable));
        }

        Set<UUID> missingUuids = Sets.difference(
                uncachedUuids,
                results.withoutRemotes().stream().map(PaxosContainer::get).collect(toSet()));

        missingUuids.forEach(missingUuid -> completeRequest(uuidsToRequests, missingUuid, Optional.empty()));
    }

    private static void completeRequest(
            Multimap<UUID, SettableFuture<Optional<ClientAwarePingableLeader>>> uuidsToRequests,
            UUID uuid,
            Optional<ClientAwarePingableLeader> outcome) {
        uuidsToRequests.get(uuid).forEach(result -> result.set(outcome));
    }

    private void throwIfInvalidSetup(
            ClientAwarePingableLeader cachedService,
            ClientAwarePingableLeader pingedService,
            UUID pingedServiceUuid) {
        if (cachedService == null) {
            return;
        }

        IllegalStateException exception = new IllegalStateException(
                "There is a fatal problem with the leadership election configuration! "
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
