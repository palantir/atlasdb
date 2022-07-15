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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProposedSolutionBuilder {
    private final Map<TokenRangeEndpoints, SweepShard> proposedSolution;
    private final int numShards;
    private int INDEX = 0;

    public ProposedSolutionBuilder(int numShards) {
        proposedSolution = new HashMap<>();
        this.numShards = numShards;
    }

    public Map<TokenRangeEndpoints, SweepShard> getProposedSolution() {
        return proposedSolution;
    }

    public void addTrivialDistribution(List<TokenRangeEndpoints> allSlices) {
        allSlices.forEach(slice -> {
            proposedSolution.put(slice, ImmutableSweepShard.of(INDEX));
            INDEX = (INDEX + 1) % numShards;
        });
    }
}
