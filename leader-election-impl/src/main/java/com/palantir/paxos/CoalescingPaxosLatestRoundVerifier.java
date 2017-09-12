/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.paxos;

import java.util.function.Function;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A verifier that coalesces verification requests for a given round, such that only one verification for a round is
 * ever running at a time.
 */
public class CoalescingPaxosLatestRoundVerifier implements PaxosLatestRoundVerifier {

    // Due to concurrency, it's possible that verifications for different rounds might be requested out of order.
    // So, we maintain a cache of {@link CoalescingSupplier}s for the two most recently requested rounds.
    // We could alternatively assume that a verification for an older round will always return
    // {@link PaxosQuorumStatus.SOME_DISAGREED}, but that depends on assumptions about how paxos works and how this
    // class will be used, which feel out of place here.
    private final LoadingCache<Long, CoalescingSupplier<PaxosQuorumStatus>> verifiersByRound;

    public CoalescingPaxosLatestRoundVerifier(PaxosLatestRoundVerifier delegate) {
        this.verifiersByRound = buildCache(
                round -> new CoalescingSupplier<>(() -> delegate.isLatestRound(round)));
    }

    private static LoadingCache<Long, CoalescingSupplier<PaxosQuorumStatus>> buildCache(
            Function<Long, CoalescingSupplier<PaxosQuorumStatus>> verifierFactory) {
        return CacheBuilder.newBuilder()
                .maximumSize(2)
                .build(new CacheLoader<Long, CoalescingSupplier<PaxosQuorumStatus>>() {
                    @Override
                    public CoalescingSupplier<PaxosQuorumStatus> load(Long round) throws Exception {
                        return verifierFactory.apply(round);
                    }
                });
    }

    @Override
    public PaxosQuorumStatus isLatestRound(long round) {
        return verifiersByRound.getUnchecked(round).get();
    }

}
