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

package com.palantir.atlasdb.keyvalue.cassandra.paging;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;

class TokenRingAwareBatchingStrategy implements RowGetterBatchingStrategy {
    private final RangeMap<LightweightOppToken, List<InetSocketAddress>> rangesToHosts;
    private final KeyRange requestedRange;

    private TokenRingAwareBatchingStrategy(
            RangeMap<LightweightOppToken, List<InetSocketAddress>> rangesToHosts,
            KeyRange requestedRange) {
        this.rangesToHosts = rangesToHosts;
        this.requestedRange = requestedRange;
    }

    static RowGetterBatchingStrategy create(CassandraClientPool cassandraClientPool,
            KeyRange requestedRange) {
        RangeMap<LightweightOppToken, List<InetSocketAddress>> ranges = cassandraClientPool.getTokenMap()
                .subRangeMap(Range.openClosed(
                        new LightweightOppToken(requestedRange.getStart_key()),
                        new LightweightOppToken(requestedRange.getEnd_key())));
        return new TokenRingAwareBatchingStrategy(ranges, requestedRange);
    }

    @Override
    public Optional<KeyRange> getNextKeyRange(Optional<KeyRange> previousKeyRange,
            List<KeySlice> previousQueryResults) {
        byte[] startingPoint = PtBytes.EMPTY_BYTE_ARRAY;
        if (previousKeyRange.isPresent()) {
            byte[] completed = previousKeyRange.get().getEnd_key();
            if (UnsignedBytes.lexicographicalComparator().compare(completed, requestedRange.getEnd_key()) >= 0) {
                return Optional.empty();
            }
            startingPoint = RangeRequests.nextLexicographicName(completed);
        }
        return Optional.ofNullable(rangesToHosts.getEntry(new LightweightOppToken(startingPoint)))
                .map(Map.Entry::getKey)
                .map(tokenRange -> new KeyRange(requestedRange)
                        .setStart_key(tokenRange.lowerEndpoint().getBytes())
                        .setEnd_key(tokenRange.upperEndpoint().getBytes()));
    }
}
