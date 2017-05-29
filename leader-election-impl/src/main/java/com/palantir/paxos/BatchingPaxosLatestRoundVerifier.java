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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.palantir.util.CoalescingExecutor;

public class BatchingPaxosLatestRoundVerifier implements PaxosLatestRoundVerifier {

    private final PaxosLatestRoundVerifier delegate;
    private final LoadingCache<Long, CoalescingExecutor<PaxosQuorumResult>> verifiersByRound;

    public BatchingPaxosLatestRoundVerifier(PaxosLatestRoundVerifier delegate) {
        this.delegate = delegate;
        this.verifiersByRound = CacheBuilder
                .newBuilder()
                .maximumSize(2)
                .build(new CacheLoader<Long, CoalescingExecutor<PaxosQuorumResult>>() {
                    @Override
                    public CoalescingExecutor<PaxosQuorumResult> load(Long round) throws Exception {
                        return new CoalescingExecutor<>(() -> delegate.isLatestRound(round));
                    }
                });
    }

    @Override
    public PaxosQuorumResult isLatestRound(long round) {
        return verifiersByRound.getUnchecked(round).getNextResult();
    }
}
