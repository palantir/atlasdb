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
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
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
import org.immutables.value.Value;

/**
 * This class batches getCommitTimestamps requests to TimeLock server for a single client/namespace.
 * */
final class BatchingCommitTimestampGetter implements CommitTimestampGetter {
    private final DisruptorAutobatcher<Request, Long> autobatcher;

    private BatchingCommitTimestampGetter(DisruptorAutobatcher<Request, Long> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static BatchingCommitTimestampGetter create(LockLeaseService leaseService, LockWatchCache cache) {
        DisruptorAutobatcher<Request, Long> autobatcher = Autobatchers.independent(consumer(leaseService, cache))
                .safeLoggablePurpose("get-commit-timestamp")
                .batchFunctionTimeout(Duration.ofSeconds(30))
                .build();
        return new BatchingCommitTimestampGetter(autobatcher);
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return AtlasFutures.getUnchecked(autobatcher.apply(ImmutableRequest.builder()
                .startTs(startTs)
                .commitLocksToken(commitLocksToken)
                .build()));
    }

    @VisibleForTesting
    static Consumer<List<BatchElement<Request, Long>>> consumer(LockLeaseService leaseService, LockWatchCache cache) {
        return batch -> {
            int count = batch.size();
            List<Long> commitTimestamps = new ArrayList<>();
            while (commitTimestamps.size() < count) {
                Optional<LockWatchVersion> requestedVersion =
                        cache.getEventCache().lastKnownVersion();
                GetCommitTimestampsResponse response =
                        leaseService.getCommitTimestamps(requestedVersion, count - commitTimestamps.size());
                commitTimestamps.addAll(process(batch.subList(commitTimestamps.size(), count), response, cache));
            }

            for (int i = 0; i < count; i++) {
                batch.get(i).result().set(commitTimestamps.get(i));
            }
        };
    }

    private static List<Long> process(
            List<BatchElement<Request, Long>> requests, GetCommitTimestampsResponse response, LockWatchCache cache) {
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
        return timestamps;
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
