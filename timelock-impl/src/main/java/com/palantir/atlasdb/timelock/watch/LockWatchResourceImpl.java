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

package com.palantir.atlasdb.timelock.watch;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.timelock.watch.trie.LousyPrefixTrieImpl;

public class LockWatchResourceImpl implements LockWatchResource {
    private static final Logger log = LoggerFactory.getLogger(LockWatchResourceImpl.class);

    private static final int WATCH_LIMIT = 1_000;

    private final BiMap<LockPredicate, WatchIdentifier> knownPredicates;
    private final Map<WatchIdentifier, LockWatch> activeWatches;

    private final LockWatchManager manager;

    public LockWatchResourceImpl() {
        this.knownPredicates = HashBiMap.create();
        this.activeWatches = Maps.newConcurrentMap();
        this.manager = new LockWatchManagerManager(ImmutableSet.of(
                new ExplicitLockWatchManager(), new PrefixLockWatchManager(new LousyPrefixTrieImpl<>())));
    }

    @Override
    public WatchStateResponse registerOrGetStates(WatchStateQuery query) {
        Set<RegisterWatchResponse> registrationStates = registerWatches(query.newPredicates());
        Map<WatchIdentifier, WatchIndexState> assumedExtantStates = getWatchStates(query.knownIdentifiers());
        return ImmutableWatchStateResponse.builder()
                .addAllRegisterResponses(registrationStates)
                .putAllStateResponses(assumedExtantStates)
                .build();
    }

    @Override
    public Set<WatchIdentifier> unregisterWatch(Set<WatchIdentifier> identifiers) {
        Set<WatchIdentifier> unregistered = Sets.newHashSet();
        for (WatchIdentifier identifier : identifiers) {
            LockWatch watch = activeWatches.remove(identifier);
            if (watch != null) {
                LockPredicate predicate = knownPredicates.inverse().remove(identifier);
                manager.unseedProcessor(predicate, watch);
                unregistered.add(identifier);
            }
        }
        return unregistered;
    }

    private Map<WatchIdentifier, WatchIndexState> getWatchStates(Set<WatchIdentifier> identifiers) {
        Map<WatchIdentifier, WatchIndexState> states = Maps.newHashMap();
        for (WatchIdentifier identifier : identifiers) {
            LockWatch watch = activeWatches.get(identifier);
            if (watch != null) {
                states.put(identifier, watch.getState());
            }
        }
        return states;
    }

    public LockEventProcessor getEventProcessor() {
        return manager.getEventProcessor();
    }

    private Set<RegisterWatchResponse> registerWatches(Set<LockPredicate> predicates) {
        // TODO (jkong): Be stricter in respecting the limit in the presence of concurrent registrations.

        Set<RegisterWatchResponse> result = Sets.newHashSet();
        // TODO (jkong): Refactor this crap
        for (LockPredicate predicate : predicates) {
            if (knownPredicates.containsKey(predicate)) {
                WatchIdentifier identifier = knownPredicates.get(predicate);
                LockWatch watch = activeWatches.get(identifier);
                result.add(ImmutableRegisterWatchResponse.builder()
                        .predicate(predicate)
                        .identifier(identifier)
                        .indexState(watch.getState())
                        .build());
                continue;
            }

            Optional<RegisterWatchResponse> watchIdentifier = registerNewWatchIdentifier(predicate);
            watchIdentifier.ifPresent(result::add);
        }
        return result;
    }

    private Optional<RegisterWatchResponse> registerNewWatchIdentifier(LockPredicate predicate) {
        // TODO (jkong): Be stricter with concurrency
        if (activeWatches.size() >= WATCH_LIMIT) {
            return Optional.empty();
        }

        UUID idToAssign = UUID.randomUUID();
        WatchIdentifier watchIdentifier = WatchIdentifier.of(idToAssign);

        LockWatch watch = new LockWatch();
        activeWatches.put(watchIdentifier, watch);
        knownPredicates.put(predicate, watchIdentifier);
        manager.seedProcessor(predicate, watch);
        return Optional.of(
                ImmutableRegisterWatchResponse.builder()
                        .predicate(predicate)
                        .identifier(watchIdentifier)
                        .indexState(watch.getState())
                        .build()
        );
    }
}
