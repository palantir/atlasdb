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

import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.BatchedStartTransactionResponse;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.v2.TimestampRangeAndPartition;
import com.palantir.timestamp.TimestampRange;

final class CoalescingTransactionService {
    private final DisruptorAutobatcher<Void, StartIdentifiedAtlasDbTransactionResponse> autobatcher;

    private CoalescingTransactionService(
            DisruptorAutobatcher<Void, StartIdentifiedAtlasDbTransactionResponse> autobatcher) {
        this.autobatcher = autobatcher;
    }

    static CoalescingTransactionService create(LockLeaseService lockLeaseService) {
        return new CoalescingTransactionService(DisruptorAutobatcher.create(batch -> {
            int numTransactions = batch.size();

            BatchedStartTransactionResponse response = lockLeaseService.batchedStartTransaction(numTransactions);
            LockToken immutableTsLock = response.immutableTimestamp().getLock();

            List<LockToken> lockTokenShares = LockTokenShare.share(immutableTsLock, numTransactions);
            List<Long> startTimestamps = getStartTimestamps(response.startTimestamps());

            for (int i = 0; i < batch.size(); i++) {
                batch.get(i).result().set(
                        StartIdentifiedAtlasDbTransactionResponse.of(
                                LockImmutableTimestampResponse.of(
                                        response.immutableTimestamp().getImmutableTimestamp(),
                                        lockTokenShares.get(i)),
                                TimestampAndPartition.of(startTimestamps.get(i), response.startTimestamps().partition()))
                );
            }}));
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

    private static List<Long> getStartTimestamps(TimestampRangeAndPartition timestampRangeAndPartition) {
        TimestampRange timestampRange = timestampRangeAndPartition.range();
        int partition = timestampRangeAndPartition.partition();
        List<Long> result = new ArrayList<>();

        for (long timestamp = timestampRange.getLowerBound(); timestamp <= timestampRange.getUpperBound(); timestamp += partition) {
            result.add(timestamp);
        }

        return result;
    }

}
