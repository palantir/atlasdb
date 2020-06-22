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
import com.palantir.lock.watch.ImmutableTransactionUpdate;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.TransactionUpdate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.immutables.value.Value;

final class CommitTimestampGetter implements AutoCloseable {
    private final DisruptorAutobatcher<Request, Long> autobatcher;

    private CommitTimestampGetter(DisruptorAutobatcher<Request, Long> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static CommitTimestampGetter create(LockLeaseService leaseService, LockWatchEventCache cache) {
        DisruptorAutobatcher<Request, Long> autobatcher = Autobatchers
                .independent(consumer(leaseService, cache))
                .safeLoggablePurpose("get-commit-timestamp")
                .build();
        return new CommitTimestampGetter(autobatcher);
    }

    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return AtlasFutures.getUnchecked(autobatcher.apply(ImmutableRequest.builder()
                .startTs(startTs)
                .commitLocksToken(commitLocksToken)
                .build()));
    }

    @VisibleForTesting
    static Consumer<List<BatchElement<Request, Long>>> consumer(LockLeaseService leaseService,
            LockWatchEventCache cache) {
        return batch -> {
            int count = batch.size();
            List<Long> commitTimestamps = new ArrayList<>();
            while (commitTimestamps.size() < count) {
                GetCommitTimestampsResponse response = leaseService.getCommitTimestamps(cache.lastKnownVersion(),
                        count - commitTimestamps.size());
                commitTimestamps.addAll(process(batch.subList(commitTimestamps.size(), count), response, cache));
            }

            for (int i = 0; i < count; i++) {
                batch.get(i).result().set(commitTimestamps.get(i));
            }
        };
    }

    private static List<Long> process(List<BatchElement<Request, Long>> requests, GetCommitTimestampsResponse response,
            LockWatchEventCache cache) {
        List<Long> timestamps = LongStream
                .rangeClosed(response.getInclusiveLower(), response.getInclusiveUpper())
                .boxed()
                .collect(Collectors.toList());
        List<TransactionUpdate> transactionUpdates = Streams.zip(timestamps.stream(), requests.stream(),
                (commitTs, batchElement) -> ImmutableTransactionUpdate.builder()
                        .startTs(batchElement.argument().startTs())
                        .commitTs(commitTs)
                        .writesToken(batchElement.argument().commitLocksToken())
                        .build()).collect(Collectors.toList());
        cache.processGetCommitTimestampsUpdate(transactionUpdates, response.getLockWatchUpdate());
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
