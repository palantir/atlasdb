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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.TimestampAndPartition;

final class CoalescingTransactionService {
    private final DisruptorAutobatcher<Void, StartIdentifiedAtlasDbTransactionResponse> autobatcher;

    private CoalescingTransactionService(
            DisruptorAutobatcher<Void, StartIdentifiedAtlasDbTransactionResponse> autobatcher) {
        this.autobatcher = autobatcher;
    }

    static CoalescingTransactionService create(LockLeaseService lockLeaseService) {
        return new CoalescingTransactionService(DisruptorAutobatcher.create(batch -> {
            int numTransactions = batch.size();

            List<StartIdentifiedAtlasDbTransactionResponse> startTransactionResponses =
                    startTransactions(lockLeaseService, numTransactions);

            for (int i = 0; i < numTransactions; i++) {
                batch.get(i).result().set(startTransactionResponses.get(i));
            }
        }));
    }

    StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction() {
        try {
            return autobatcher.apply(null).get();
        } catch (ExecutionException e) {
            throw Throwables.throwUncheckedException(e.getCause());
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    private static List<StartIdentifiedAtlasDbTransactionResponse> startTransactions(
            LockLeaseService lockLeaseService, int numberOfTransactions) {
        List<StartIdentifiedAtlasDbTransactionResponse> result = new ArrayList<>();
        while (result.size() < numberOfTransactions) {
            result.addAll(split(lockLeaseService.startTransactions(numberOfTransactions - result.size())));
        }
        return result.subList(0, numberOfTransactions);
    }

    @VisibleForTesting
    static List<StartIdentifiedAtlasDbTransactionResponse> split(StartTransactionResponseV4 batchedResponse) {
        LockImmutableTimestampResponse immutableTsAndLock = batchedResponse.immutableTimestamp();
        PartitionedTimestamps partitionedTimestamps = batchedResponse.timestamps();
        int partition = partitionedTimestamps.partition();

        Stream<LockImmutableTimestampResponse> immutableTsAndLocks =
                LockTokenShare.share(immutableTsAndLock.getLock(), partitionedTimestamps.count()).stream()
                        .map(token ->
                                LockImmutableTimestampResponse.of(immutableTsAndLock.getImmutableTimestamp(), token));

        Stream<TimestampAndPartition> timestampAndPartitions = partitionedTimestamps.stream()
                .mapToObj(timestamp -> TimestampAndPartition.of(timestamp, partition));

        return Streams.zip(immutableTsAndLocks, timestampAndPartitions,
                StartIdentifiedAtlasDbTransactionResponse::of)
                .collect(Collectors.toList());
    }
}
