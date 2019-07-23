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

package com.palantir.atlasdb.transaction.impl.illiteracy;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.timelock.watch.ExplicitLockPredicate;
import com.palantir.atlasdb.timelock.watch.ImmutableWatchStateQuery;
import com.palantir.atlasdb.timelock.watch.LockPredicate;
import com.palantir.atlasdb.timelock.watch.LockWatchRpcClient;
import com.palantir.atlasdb.timelock.watch.RegisterWatchResponse;
import com.palantir.atlasdb.timelock.watch.WatchIdentifier;
import com.palantir.atlasdb.timelock.watch.WatchIndexState;
import com.palantir.atlasdb.timelock.watch.WatchStateQuery;
import com.palantir.atlasdb.timelock.watch.WatchStateResponse;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.LockDescriptor;

public class RemoteLockWatchClient {
    private final LockWatchRpcClient lockWatchRpcClient;
    private final ConcurrentMap<RowReference, WatchIdentifier> knownWatchIdentifiers;

    public RemoteLockWatchClient(LockWatchRpcClient lockWatchRpcClient) {
        this.lockWatchRpcClient = lockWatchRpcClient;
        knownWatchIdentifiers = Maps.newConcurrentMap();
    }

    public Map<RowReference, WatchIdentifierAndState> getStateForRows(Set<RowReference> rowReferences) {
        try {
            return getStateForRowsUnsafe(rowReferences);
        } catch (Exception e) {
            // If we couldn't talk to the lock watch service, don't explode in a blaze of glory
            // Just return nothing, which means we read everything from the KVS
            return ImmutableMap.of();
        }
    }

    // Precondition: The table reference in this row reference MUST use row level locking!
    // If you don't use that you WILL see weirdness, and probably P0 yourself :(
    private Map<RowReference, WatchIdentifierAndState> getStateForRowsUnsafe(Set<RowReference> rowReferences) {
        // TODO (jkong): Optimize the concurrency model
        Map<RowReference, WatchIdentifier> recognisedReferences = getRecognisedReferences(rowReferences);
        Map<LockPredicate, RowReference> unrecognisedPredicates =
                KeyedStream.of(Sets.difference(rowReferences, recognisedReferences.keySet()))
                        .mapKeys(RowReference::toLockDescriptor)
                        .mapKeys((Function<LockDescriptor, LockPredicate>) ExplicitLockPredicate::of)
                        .collectToMap();
        WatchStateQuery query = ImmutableWatchStateQuery.builder()
                .addAllKnownIdentifiers(recognisedReferences.values())
                .addAllNewPredicates(unrecognisedPredicates.keySet())
                .build();

        WatchStateResponse response = lockWatchRpcClient.registerOrGetStates(query);
        if (!response.getStateResponses().keySet().containsAll(recognisedReferences.values())) {
            return reregisterAllWatches(rowReferences, unrecognisedPredicates);
        }

        // happy path
        Map<RowReference, WatchIdentifierAndState> answer = Maps.newHashMap();
        for (Map.Entry<RowReference, WatchIdentifier> entry : recognisedReferences.entrySet()) {
            WatchIndexState state = response.getStateResponses().get(entry.getValue());
            answer.put(entry.getKey(), WatchIdentifierAndState.of(entry.getValue(), state));
        }
        for (RegisterWatchResponse registerResponse : response.registerResponses()) {
            RowReference reference = unrecognisedPredicates.get(registerResponse.predicate());
            if (reference != null) {
                answer.put(reference, WatchIdentifierAndState.of(
                        registerResponse.identifier(), registerResponse.indexState()));
                knownWatchIdentifiers.put(reference, registerResponse.identifier());
            }
        }
        return answer;
    }

    private Map<RowReference, WatchIdentifierAndState> reregisterAllWatches(Set<RowReference> rowReferences,
            Map<LockPredicate, RowReference> unrecognisedPredicates) {
        // an election happened, or something got unregistered :/
        Map<LockPredicate, RowReference> allPredicates = KeyedStream.of(rowReferences)
                .mapKeys(RowReference::toLockDescriptor)
                .mapKeys((Function<LockDescriptor, LockPredicate>) ExplicitLockPredicate::of)
                .collectToMap();
        WatchStateQuery newQuery = ImmutableWatchStateQuery.builder()
                .addAllNewPredicates(allPredicates.keySet())
                .build();
        Map<RowReference, WatchIdentifierAndState> answer = Maps.newHashMap();
        WatchStateResponse newResponse = lockWatchRpcClient.registerOrGetStates(newQuery);
        for (RegisterWatchResponse registerResponse : newResponse.registerResponses()) {
            RowReference reference = allPredicates.get(registerResponse.predicate());
            answer.put(reference, WatchIdentifierAndState.of(
                    registerResponse.identifier(), registerResponse.indexState()));
            knownWatchIdentifiers.put(reference, registerResponse.identifier());
        }
        return answer;
    }

    private Map<RowReference, WatchIdentifier> getRecognisedReferences(Set<RowReference> rowReferences) {
        // TODO (jkong): Lots of copying for something not written often!
        // TODO (jkong): Replace with vavr collections or something like that
        Map<RowReference, WatchIdentifier> map = ImmutableMap.copyOf(knownWatchIdentifiers);
        Map<RowReference, WatchIdentifier> result = Maps.newHashMap();
        for (RowReference rowReference : rowReferences) {
            WatchIdentifier identifier = map.get(rowReference);
            if (identifier != null) {
                result.put(rowReference, identifier);
            }
        }
        return result;
    }

    public WatchIdentifierAndState getStateForCell(CellReference cellReference) {
        throw new UnsupportedOperationException("not yet");
    }
}
