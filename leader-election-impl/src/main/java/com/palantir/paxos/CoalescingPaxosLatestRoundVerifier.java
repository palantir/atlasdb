/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.paxos;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.common.concurrent.CoalescingSupplier;

/**
 * A verifier that coalesces verification requests for a given round, such that only one verification for a round is
 * ever running at a time.
 */
public class CoalescingPaxosLatestRoundVerifier implements PaxosLatestRoundVerifier {

    private final PaxosLatestRoundVerifier delegate;
    // we only care about keeping the verifier for the latest round; the cache is just here to handle concurrency
    // around creating a new verifier for a newly requested round
    private final LoadingCache<Long, CoalescingSupplier<PaxosQuorumStatus>> verifiersByRound = CacheBuilder.newBuilder()
            .maximumSize(1)
            .build(new CacheLoader<Long, CoalescingSupplier<PaxosQuorumStatus>>() {
                @Override
                public CoalescingSupplier<PaxosQuorumStatus> load(Long key) throws Exception {
                    return new CoalescingSupplier<>(() -> delegate.isLatestRound(key));
                }
            });

    public CoalescingPaxosLatestRoundVerifier(PaxosLatestRoundVerifier delegate) {
        this.delegate = delegate;
    }

    @Override
    public ListenableFuture<PaxosQuorumStatus> isLatestRoundAsync(long round) {
        return verifiersByRound.getUnchecked(round).getAsync();
    }

    @Override
    public PaxosQuorumStatus isLatestRound(long round) {
        return verifiersByRound.getUnchecked(round).get();
    }
}
