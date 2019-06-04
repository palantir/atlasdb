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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;

class TokenRingAwareBatchingStrategy implements RowGetterBatchingStrategy {
    // These ranges are always open -> closed (or open-open in the case of the last range).
    private final RangeMap<LightweightOppToken, List<InetSocketAddress>> rangesToHosts;
    private final KeyRange requestedRange;
    private final int batchSize;

    private TokenRingAwareBatchingStrategy(
            RangeMap<LightweightOppToken, List<InetSocketAddress>> rangesToHosts,
            KeyRange requestedRange,
            int batchSize) {
        this.rangesToHosts = classifyUnknownRangesSeparately(rangesToHosts);
        this.requestedRange = requestedRange;
        this.batchSize = batchSize;
    }

    private RangeMap<LightweightOppToken, List<InetSocketAddress>> classifyUnknownRangesSeparately(
            RangeMap<LightweightOppToken, List<InetSocketAddress>> rangesToHosts) {
        return ImmutableRangeMap.builder()
                .putAll(rangesToHosts)
                .build();
    }

    static RowGetterBatchingStrategy create(CassandraClientPool cassandraClientPool,
            KeyRange requestedRange,
            SlicePredicate predicate) {
        RangeMap<LightweightOppToken, List<InetSocketAddress>> ranges = cassandraClientPool.getTokenMap()
                .subRangeMap(convertKeyRangeToRange(requestedRange));
        if (ranges.asMapOfRanges().isEmpty()) {
            // cassandra kvs ranges are not available yet
            return RowGetterBatchingStrategy.naiveStrategy(requestedRange);
        }
        return new TokenRingAwareBatchingStrategy(ranges, requestedRange, predicate.slice_range.getCount());
    }

    @Override
    public Optional<KeyRange> getNextKeyRange(Optional<KeyRange> previousKeyRange,
            List<KeySlice> previousQueryResults) {
        if (!previousKeyRange.isPresent()) {
            // Make the first query
            byte[] startingPoint = requestedRange.getStart_key();
            System.out.println(new LightweightOppToken(startingPoint));
            System.out.println(rangesToHosts);
            System.out.println(rangesToHosts.getEntry(new LightweightOppToken(startingPoint)));
            return Optional.ofNullable(rangesToHosts.getEntry(new LightweightOppToken(startingPoint)))
                    .map(Map.Entry::getKey)
                    .map(tokenRange -> copyKeyRangeWithNewBounds(requestedRange, tokenRange.intersection(
                            convertKeyRangeToRange(requestedRange))));
        }

        KeyRange presentKeyRange = previousKeyRange.get();
        if (previousQueryResults.size() >= batchSize && previousQueryResults.size() > 0) {
            if (Arrays.equals(PtBytes.EMPTY_BYTE_ARRAY, presentKeyRange.getEnd_key())) {
                // Done with the last range
                return Optional.empty();
            }
            // Maybe more results in the same range, but only test after where we are
            return Optional.of(copyKeyRangeWithNewBounds(presentKeyRange, Range.range(
                    new LightweightOppToken(previousQueryResults.get(previousQueryResults.size() - 1).getKey()),
                    BoundType.OPEN,
                    new LightweightOppToken(presentKeyRange.getEnd_key()),
                    BoundType.OPEN)));
        }

        // No more results in this range
        byte[] nextStartingPoint = presentKeyRange.getEnd_key();
        if (nextStartingPoint != PtBytes.EMPTY_BYTE_ARRAY) {
            return Optional.ofNullable(rangesToHosts.getEntry(new LightweightOppToken(
                    RangeRequests.nextLexicographicName(RangeRequests.nextLexicographicName(nextStartingPoint)))))
                    .map(Map.Entry::getKey)
                    .map(tokenRange -> copyKeyRangeWithNewBounds(requestedRange, tokenRange));
        }

        return Optional.empty();
    }

    private static Range<LightweightOppToken> convertKeyRangeToRange(KeyRange requestedRange) {
        if (Arrays.equals(requestedRange.getStart_key(), PtBytes.EMPTY_BYTE_ARRAY)
                && Arrays.equals(requestedRange.getEnd_key(), PtBytes.EMPTY_BYTE_ARRAY)) {
            return Range.all();
        }
        if (Arrays.equals(requestedRange.getEnd_key(), PtBytes.EMPTY_BYTE_ARRAY)) {
            return Range.atLeast(new LightweightOppToken(requestedRange.getStart_key()));
        }
        return Range.openClosed(
                new LightweightOppToken(requestedRange.getStart_key()),
                new LightweightOppToken(requestedRange.getEnd_key()));
    }

    private static KeyRange copyKeyRangeWithNewBounds(KeyRange base, Range<LightweightOppToken> tokenRange) {
        KeyRange copy = new KeyRange(base);
        if (tokenRange.hasLowerBound()) {
            copy.setStart_key(tokenRange.lowerBoundType() == BoundType.CLOSED
                    ? tokenRange.lowerEndpoint().getBytes()
                    : RangeRequests.nextLexicographicName(tokenRange.lowerEndpoint().getBytes()));
        } else {
            copy.setStart_key(PtBytes.EMPTY_BYTE_ARRAY);
        }
        if (tokenRange.hasUpperBound()) {
            copy.setEnd_key(tokenRange.upperBoundType() == BoundType.CLOSED
                    ? tokenRange.upperEndpoint().getBytes()
                    : RangeRequests.previousLexicographicName(tokenRange.upperEndpoint().getBytes()));
        } else {
            copy.setEnd_key(PtBytes.EMPTY_BYTE_ARRAY);
        }
        return copy;
    }

}
