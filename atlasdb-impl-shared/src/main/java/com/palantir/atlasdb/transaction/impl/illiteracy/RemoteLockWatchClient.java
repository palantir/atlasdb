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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.timelock.watch.ImmutableExplicitLockPredicate;
import com.palantir.atlasdb.timelock.watch.ImmutableLockDescriptorPrefix;
import com.palantir.atlasdb.timelock.watch.ImmutablePrefixLockPredicate;
import com.palantir.atlasdb.timelock.watch.ImmutableWatchStateQuery;
import com.palantir.atlasdb.timelock.watch.LockPredicate;
import com.palantir.atlasdb.timelock.watch.LockWatchRpcClient;
import com.palantir.atlasdb.timelock.watch.RegisterWatchResponse;
import com.palantir.atlasdb.timelock.watch.WatchIdentifier;
import com.palantir.atlasdb.timelock.watch.WatchIndexState;
import com.palantir.atlasdb.timelock.watch.WatchStateQuery;
import com.palantir.atlasdb.timelock.watch.WatchStateResponse;
import com.palantir.common.streams.KeyedStream;

public class RemoteLockWatchClient {
    private final LockWatchRpcClient lockWatchRpcClient;
    private final ConcurrentMap<RowCacheReference, WatchIdentifier> knownWatchIdentifiers;

    public RemoteLockWatchClient(LockWatchRpcClient lockWatchRpcClient) {
        this.lockWatchRpcClient = lockWatchRpcClient;
        knownWatchIdentifiers = Maps.newConcurrentMap();
    }

    public Map<RowCacheReference, WatchIdentifierAndState> getCacheStateForCacheReferences(
            Set<RowCacheReference> rowCacheReferences) {
        try {
            return getStateForRowsUnsafe(rowCacheReferences);
        } catch (Exception e) {
            // If we couldn't talk to the lock watch service, don't explode in a blaze of glory
            // Just return nothing, which means we read everything from the KVS
            return ImmutableMap.of();
        }
    }

    // Precondition: The table reference in this row reference MUST use row level locking!
    // If you don't use that you WILL see weirdness, and probably P0 yourself :(
    private Map<RowCacheReference, WatchIdentifierAndState> getStateForRowsUnsafe(
            Set<RowCacheReference> rowCacheReferences) {
        // TODO (jkong): Optimize the concurrency model
        Map<RowCacheReference, WatchIdentifier> recognisedReferences = getRecognisedReferences(rowCacheReferences);
        Map<LockPredicate, RowCacheReference> unrecognisedPredicates =
                KeyedStream.of(Sets.difference(rowCacheReferences, recognisedReferences.keySet()))
                        .mapKeys(RemoteLockWatchClient::convertToPredicate)
                        .collectToMap();
        WatchStateQuery query = ImmutableWatchStateQuery.builder()
                .addAllKnownIdentifiers(recognisedReferences.values())
                .addAllNewPredicates(unrecognisedPredicates.keySet())
                .build();

        WatchStateResponse response = lockWatchRpcClient.registerOrGetStates(query);
        if (!response.getStateResponses().keySet().containsAll(recognisedReferences.values())) {
            return reregisterAllWatches(rowCacheReferences);
        }

        // happy path
        Map<RowCacheReference, WatchIdentifierAndState> answer = Maps.newHashMap();
        for (Map.Entry<RowCacheReference, WatchIdentifier> entry : recognisedReferences.entrySet()) {
            WatchIndexState state = response.getStateResponses().get(entry.getValue());
            answer.put(entry.getKey(), WatchIdentifierAndState.of(entry.getValue(), state));
        }
        for (RegisterWatchResponse registerResponse : response.registerResponses()) {
            RowCacheReference reference = unrecognisedPredicates.get(registerResponse.predicate());
            if (reference != null) {
                answer.put(reference, WatchIdentifierAndState.of(
                        registerResponse.identifier(), registerResponse.indexState()));
                knownWatchIdentifiers.put(reference, registerResponse.identifier());
            }
        }
        return answer;
    }

    private Map<RowCacheReference, WatchIdentifierAndState> reregisterAllWatches(Set<RowCacheReference> rowReferences) {
        // an election happened, or something got unregistered :/
        Map<LockPredicate, RowCacheReference> allPredicates = KeyedStream.of(rowReferences)
                .mapKeys(RemoteLockWatchClient::convertToPredicate)
                .collectToMap();
        WatchStateQuery newQuery = ImmutableWatchStateQuery.builder()
                .addAllNewPredicates(allPredicates.keySet())
                .build();
        Map<RowCacheReference, WatchIdentifierAndState> answer = Maps.newHashMap();
        WatchStateResponse newResponse = lockWatchRpcClient.registerOrGetStates(newQuery);
        for (RegisterWatchResponse registerResponse : newResponse.registerResponses()) {
            RowCacheReference reference = allPredicates.get(registerResponse.predicate());
            answer.put(reference, WatchIdentifierAndState.of(
                    registerResponse.identifier(), registerResponse.indexState()));
            knownWatchIdentifiers.put(reference, registerResponse.identifier());
        }
        return answer;
    }

    private Map<RowCacheReference, WatchIdentifier> getRecognisedReferences(Set<RowCacheReference> rowCacheRefs) {
        // TODO (jkong): Lots of copying for something not written often!
        // TODO (jkong): Replace with vavr collections or something like that
        Map<RowCacheReference, WatchIdentifier> map = ImmutableMap.copyOf(knownWatchIdentifiers);
        Map<RowCacheReference, WatchIdentifier> result = Maps.newHashMap();
        for (RowCacheReference rowReference : rowCacheRefs) {
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

    private static LockPredicate convertToPredicate(RowCacheReference rowCacheReference) {
        if (rowCacheReference.rowReference().isPresent()) {
            return ImmutableExplicitLockPredicate.builder()
                    .addDescriptors(rowCacheReference.rowReference().get().toLockDescriptor()).build();
        }
        System.out.println("pred=" + rowCacheReference.prefixReference().get().toPrefixForm());
        return ImmutablePrefixLockPredicate.builder()
                .prefix(ImmutableLockDescriptorPrefix.builder()
                        .prefix(rowCacheReference.prefixReference().get().toPrefixForm())
                        .build())
                .build();
    }
}
