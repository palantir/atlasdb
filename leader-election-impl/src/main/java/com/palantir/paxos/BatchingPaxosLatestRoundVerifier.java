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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class BatchingPaxosLatestRoundVerifier implements PaxosLatestRoundVerifier {

    private final LoadingCache<Long, BatchingSupplier<PaxosQuorumResult>> verificationsByRound;

    public BatchingPaxosLatestRoundVerifier(PaxosLatestRoundVerifier delegate) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        this.verificationsByRound = buildCache(
                round -> new BatchingSupplier<>(() -> delegate.isLatestRound(round), executor));
    }

    @VisibleForTesting
    BatchingPaxosLatestRoundVerifier(Function<Long, BatchingSupplier<PaxosQuorumResult>> verifierFactory) {
        this.verificationsByRound = buildCache(verifierFactory);
    }

    private static LoadingCache<Long, BatchingSupplier<PaxosQuorumResult>> buildCache(
            Function<Long, BatchingSupplier<PaxosQuorumResult>> verifierFactory) {
        return CacheBuilder
                .newBuilder()
                .maximumSize(2)
                .build(new CacheLoader<Long, BatchingSupplier<PaxosQuorumResult>>() {
                    @Override
                    public BatchingSupplier<PaxosQuorumResult> load(Long round) throws Exception {
                        return verifierFactory.apply(round);
                    }
                });
    }

    @Override
    public PaxosQuorumResult isLatestRound(long round) {
        BatchingSupplier<PaxosQuorumResult> verification = verificationsByRound.getUnchecked(round);
        return getUnchecked(verification.get());
    }

    private PaxosQuorumResult getUnchecked(Future<PaxosQuorumResult> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }
}
