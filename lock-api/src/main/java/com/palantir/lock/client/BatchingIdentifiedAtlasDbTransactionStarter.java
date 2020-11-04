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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.logsafe.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BatchingIdentifiedAtlasDbTransactionStarter implements IdentifiedAtlasDbTransactionStarter {
    private final DisruptorAutobatcher<Integer, List<StartIdentifiedAtlasDbTransactionResponse>> autobatcher;

    public BatchingIdentifiedAtlasDbTransactionStarter(
            DisruptorAutobatcher<Integer, List<StartIdentifiedAtlasDbTransactionResponse>> autobatcher) {
        this.autobatcher = autobatcher;
    }

    static BatchingIdentifiedAtlasDbTransactionStarter create(
            LockLeaseService lockLeaseService, LockWatchEventCache lockWatchEventCache) {
        DisruptorAutobatcher<Integer, List<StartIdentifiedAtlasDbTransactionResponse>> autobatcher =
                Autobatchers.independent(consumer(lockLeaseService, lockWatchEventCache))
                        .safeLoggablePurpose("transaction-starter")
                        .build();
        return new BatchingIdentifiedAtlasDbTransactionStarter(autobatcher);
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        Preconditions.checkArgument(count > 0, "Cannot start 0 or fewer transactions");
        return AtlasFutures.getUnchecked(autobatcher.apply(count));
    }

    @VisibleForTesting
    static Consumer<List<BatchElement<Integer, List<StartIdentifiedAtlasDbTransactionResponse>>>> consumer(
            LockLeaseService lockLeaseService, LockWatchEventCache lockWatchEventCache) {
        return batch -> {
            int numTransactions =
                    batch.stream().mapToInt(BatchElement::argument).reduce(0, Integer::sum);

            List<StartIdentifiedAtlasDbTransactionResponse> startTransactionResponses =
                    getStartTransactionResponses(lockLeaseService, lockWatchEventCache, numTransactions);

            int start = 0;
            for (BatchElement<Integer, List<StartIdentifiedAtlasDbTransactionResponse>> batchElement : batch) {
                int end = start + batchElement.argument();
                batchElement.result().set(ImmutableList.copyOf(startTransactionResponses.subList(start, end)));
                start = end;
            }
        };
    }

    private static List<StartIdentifiedAtlasDbTransactionResponse> getStartTransactionResponses(
            LockLeaseService lockLeaseService, LockWatchEventCache lockWatchEventCache, int numberOfTransactions) {
        List<StartIdentifiedAtlasDbTransactionResponse> result = new ArrayList<>();
        while (result.size() < numberOfTransactions) {
            try {
                Optional<LockWatchVersion> requestedVersion = lockWatchEventCache.lastKnownVersion();
                ConjureStartTransactionsResponse response = lockLeaseService.startTransactionsWithWatches(
                        requestedVersion, numberOfTransactions - result.size());
                lockWatchEventCache.processStartTransactionsUpdate(
                        response.getTimestamps().stream().boxed().collect(Collectors.toSet()),
                        response.getLockWatchUpdate());
                result.addAll(split(response));
                LockWatchLogUtility.logTransactionEvents(requestedVersion, response.getLockWatchUpdate());
            } catch (Throwable t) {
                TransactionStarterHelper.unlock(
                        result.stream()
                                .map(response -> response.immutableTimestamp().getLock())
                                .collect(Collectors.toSet()),
                        lockLeaseService);
                throw Throwables.throwUncheckedException(t);
            }
        }
        return result;
    }

    private static List<StartIdentifiedAtlasDbTransactionResponse> split(ConjureStartTransactionsResponse response) {
        PartitionedTimestamps partitionedTimestamps = response.getTimestamps();
        int partition = partitionedTimestamps.partition();

        LockToken immutableTsLock = response.getImmutableTimestamp().getLock();
        long immutableTs = response.getImmutableTimestamp().getImmutableTimestamp();

        Stream<LockImmutableTimestampResponse> immutableTsAndLocks = LockTokenShare.share(
                        immutableTsLock, partitionedTimestamps.count())
                .map(tokenShare -> LockImmutableTimestampResponse.of(immutableTs, tokenShare));

        Stream<TimestampAndPartition> timestampAndPartitions =
                partitionedTimestamps.stream().mapToObj(timestamp -> TimestampAndPartition.of(timestamp, partition));

        return Streams.zip(immutableTsAndLocks, timestampAndPartitions, StartIdentifiedAtlasDbTransactionResponse::of)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        autobatcher.close();
    }
}
