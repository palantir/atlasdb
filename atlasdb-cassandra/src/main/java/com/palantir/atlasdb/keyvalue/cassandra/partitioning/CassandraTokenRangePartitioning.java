/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.partitioning;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.atlasdb.keyvalue.cassandra.pool.TokenRanges;
import com.palantir.logsafe.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("UnstableApiUsage")
public final class CassandraTokenRangePartitioning {
    private final RangeMap<LightweightOppToken, SweepShard> proposedSpread;

    public CassandraTokenRangePartitioning(int shards, TokenRanges tokenRanges) {
        this.proposedSpread = compute(shards, tokenRanges);
    }

    private RangeMap<LightweightOppToken, SweepShard> compute(int shards, TokenRanges tokenRanges) {
        Set<Integer> numHostsPerAz = tokenRanges.availabilityZoneToHosts().values().stream()
                .map(List::size)
                .collect(Collectors.toSet());

        // todo(snanda): would probably not want to throw here
        Preconditions.checkState(numHostsPerAz.size() == 1, "Expect the same number of hosts in all AZs");

        Map<TokenRangeEndpoints, SweepShard> tokenRangeSliceToShard = PartitioningAlgorithm.partitionHosts(
                        tokenRanges.availabilityZoneToHosts(), shards)
                .getProposedSolution();

        ImmutableRangeMap.Builder<LightweightOppToken, SweepShard> spreadBuilder = ImmutableRangeMap.builder();

        for (Entry<Range<LightweightOppToken>, ? extends Set<CassandraServer>> e :
                tokenRanges.tokenMap().asMapOfRanges().entrySet()) {
            TokenRangeEndpoints endpoints = ImmutableTokenRangeEndpointDetails.of(e.getValue());
            spreadBuilder.put(e.getKey(), tokenRangeSliceToShard.get(endpoints));
        }

        return spreadBuilder.build();
    }

    public int getShardForToken(byte[] key) {
        return proposedSpread.get(new LightweightOppToken(key)).id();
    }
}
