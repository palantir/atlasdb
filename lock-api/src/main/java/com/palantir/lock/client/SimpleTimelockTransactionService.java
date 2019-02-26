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

package com.palantir.lock.client;

import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;

public class SimpleTimelockTransactionService implements TimelockTransactionService {
    private LeasingTimelockClient delegate;

    public SimpleTimelockTransactionService(LeasingTimelockClient delegate) {
        this.delegate = delegate;
    }


    @Override
    public StartIdentifiedAtlasDbTransactionResponse startTransaction() {
        StartIdentifiedAtlasDbTransactionResponse response = delegate.startIdentifiedAtlasDbTransaction();
        return StartIdentifiedAtlasDbTransactionResponse.of(
                LockImmutableTimestampResponse.of(response.immutableTimestamp().getImmutableTimestamp(), wrapToken(response.immutableTimestamp().getLock())),
                response.startTimestampAndPartition());
    }

    @Override
    public Set<LockToken> reduceForRefresh(Set<LockToken> tokens) {
        Set<HeldImmutableTsToken> immutableTsTokens = filterImmutableTsTokens(tokens);
        Set<LockToken> reducedImmutableTsTokens = immutableTsTokens.stream()
                .map(h -> h.sharedToken().token())
                .collect(Collectors.toSet());

        return Sets.union(filterOutImmutableTsTokens(tokens), reducedImmutableTsTokens);
    }

    @Override
    public Set<LockToken> reduceForUnlock(Set<LockToken> tokens) {
        Set<HeldImmutableTsToken> immutableTsTokens = filterImmutableTsTokens(tokens);
        immutableTsTokens.forEach(HeldImmutableTsToken::invalidate);
        Set<LockToken> toUnlock;
        synchronized (this) {
            toUnlock = immutableTsTokens.stream().map(h -> h.sharedToken())
                    .filter(SharedImmutableTsToken::dereferenced)
                    .collect(Collectors.toSet());
        }

        return Sets.union(filterImmutableTsTokens(tokens), toUnlock);
    }

    private HeldImmutableTsToken wrapToken(LockToken lockToken) {
        return HeldImmutableTsToken.of(new SharedImmutableTsToken(lockToken));
    }

    private Set<HeldImmutableTsToken> filterImmutableTsTokens(Set<LockToken> tokens) {
        return tokens.stream().filter(t -> t instanceof HeldImmutableTsToken)
                .map(t -> (HeldImmutableTsToken) t)
                .collect(Collectors.toSet());
    }

    private Set<LockToken> filterOutImmutableTsTokens(Set<LockToken> tokens) {
        return tokens.stream().filter(t -> !(t instanceof HeldImmutableTsToken))
                .collect(Collectors.toSet());
    }


}
