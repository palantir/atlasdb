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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.palantir.lock.LockDescriptor;

public class LockWatchResourceImpl implements LockWatchResource {
    private static final int WATCH_LIMIT = 50;

    private final BiMap<LockPredicate, WatchIdentifier> knownPredicates;
    private final Multimap<LockDescriptor, LockWatch> explicitDescriptorsToWatches;
    private final Map<WatchIdentifier, LockWatch> activeWatches;
    private final LockEventProcessor eventProcessor;

    public LockWatchResourceImpl() {
        this.knownPredicates = HashBiMap.create();
        this.explicitDescriptorsToWatches = Multimaps.synchronizedListMultimap(MultimapBuilder.hashKeys()
                .arrayListValues()
                .build());
        this.activeWatches = Maps.newConcurrentMap();
        this.eventProcessor = new LockEventProcessor() {
            @Override
            public void registerLock(LockDescriptor descriptor) {
                System.out.println(activeWatches);
                explicitDescriptorsToWatches.get(descriptor).forEach(LockWatch::registerLock);
            }

            @Override
            public void registerUnlock(LockDescriptor descriptor) {
                explicitDescriptorsToWatches.get(descriptor).forEach(LockWatch::registerUnlock);
            }
        };
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
                knownPredicates.inverse().remove(identifier);
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
        return eventProcessor;
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

        // TODO (jkong): Handle prefix scans
        if (!(predicate instanceof ExplicitLockPredicate)) {
            throw new IllegalArgumentException("Non-explicit lock predicates not supported yet!");
        }
        ExplicitLockPredicate explicitLockPredicate = (ExplicitLockPredicate) predicate;

        Set<LockDescriptor> descriptors = explicitLockPredicate.descriptors();
        LockWatch watch = new LockWatch();
        activeWatches.put(watchIdentifier, watch);
        knownPredicates.put(predicate, watchIdentifier);
        descriptors.forEach(descriptor -> explicitDescriptorsToWatches.put(descriptor, watch));
        return Optional.of(
                ImmutableRegisterWatchResponse.builder()
                        .predicate(predicate)
                        .identifier(watchIdentifier)
                        .indexState(watch.getState())
                        .build()
        );
    }
}
