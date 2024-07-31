/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.GetCommitTimestampResponse;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.immutables.value.Value;

/**
 * This class batches getCommitTimestamps requests to TimeLock server for a single client/namespace.
 */
final class BatchingCommitTimestampGetter implements CommitTimestampGetter {
    private final DisruptorAutobatcher<Request, GetCommitTimestampResponse> autobatcher;

    private BatchingCommitTimestampGetter(DisruptorAutobatcher<Request, GetCommitTimestampResponse> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static BatchingCommitTimestampGetter create(LockLeaseService leaseService, LockWatchCache cache) {
        DisruptorAutobatcher<Request, GetCommitTimestampResponse> autobatcher = Autobatchers.independent(
                        consumer(leaseService, cache))
                .safeLoggablePurpose("get-commit-timestamp")
                .batchFunctionTimeout(Duration.ofSeconds(30))
                .build();
        return new BatchingCommitTimestampGetter(autobatcher);
    }

    @Override
    public GetCommitTimestampResponse getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return AtlasFutures.getUnchecked(autobatcher.apply(ImmutableRequest.builder()
                .startTs(startTs)
                .commitLocksToken(commitLocksToken)
                .build()));
    }

    @VisibleForTesting
    static Consumer<List<BatchElement<Request, GetCommitTimestampResponse>>> consumer(
            LockLeaseService leaseService, LockWatchCache cache) {
        return batch -> {
            int count = batch.size();
            List<GetCommitTimestampResponse> commitTimestamps = getResponses(leaseService, cache, batch, count);
            for (int i = 0; i < count; i++) {
                batch.get(i).result().set(commitTimestamps.get(i));
            }
        };
    }

    private static List<GetCommitTimestampResponse> getResponses(
            LockLeaseService leaseService,
            LockWatchCache cache,
            List<BatchElement<Request, GetCommitTimestampResponse>> batch,
            int count) {
        List<GetCommitTimestampResponse> commitTimestamps = new ArrayList<>();
        try {
            while (commitTimestamps.size() < count) {
                Optional<LockWatchVersion> requestedVersion =
                        cache.getEventCache().lastKnownVersion();
                GetCommitTimestampsResponse response =
                        leaseService.getCommitTimestamps(requestedVersion, count - commitTimestamps.size());
                commitTimestamps.addAll(process(batch.subList(commitTimestamps.size(), count), response, cache));
            }
            return commitTimestamps;
        } catch (Throwable t) {
            // Something else should cleanup the caches, that's fine. The joys of underhanded design.
            // But we need to unlock the commit immutable timestamp locks.
            // This method call is weird, but this is what BatchingIdentifiedAtlasDbTransactionStarter does,
            // so I'll allow it.
            TransactionStarterHelper.unlock(
                    commitTimestamps.stream()
                            .map(response -> response.immutableTimestamp().getLock())
                            .collect(Collectors.toSet()),
                    leaseService);
            throw Throwables.throwUncheckedException(t);
        }
    }

    private static List<GetCommitTimestampResponse> process(
            List<BatchElement<Request, GetCommitTimestampResponse>> requests,
            GetCommitTimestampsResponse response,
            LockWatchCache cache) {
        LockToken immutableTsLock = response.getCommitImmutableTimestamp().getLock();
        long commitImmutableTs = response.getCommitImmutableTimestamp().getImmutableTimestamp();
        Stream<LockImmutableTimestampResponse> immutableTsAndLocks = LockTokenShare.share(
                        immutableTsLock,
                        Ints.checkedCast(response.getInclusiveUpper() - response.getInclusiveLower() + 1))
                .map(tokenShare -> LockImmutableTimestampResponse.of(commitImmutableTs, tokenShare));
        List<Long> timestamps = LongStream.rangeClosed(response.getInclusiveLower(), response.getInclusiveUpper())
                .boxed()
                .collect(Collectors.toList());
        List<TransactionUpdate> transactionUpdates = Streams.zip(
                        timestamps.stream(), requests.stream(), (commitTs, batchElement) -> TransactionUpdate.builder()
                                .startTs(batchElement.argument().startTs())
                                .commitTs(commitTs)
                                .writesToken(batchElement.argument().commitLocksToken())
                                .build())
                .collect(Collectors.toList());
        cache.processCommitTimestampsUpdate(transactionUpdates, response.getLockWatchUpdate());
        return Streams.zip(immutableTsAndLocks, timestamps.stream(), GetCommitTimestampResponse::of)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    @Value.Immutable
    interface Request {
        long startTs();

        LockToken commitLocksToken();
    }
}
