/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.timelock.partition;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;

public class GreedyTimeLockPartitioner implements TimeLockPartitioner {
    private final int miniclusterSize;

    public GreedyTimeLockPartitioner(int miniclusterSize) {
        this.miniclusterSize = miniclusterSize;
    }

    @Override
    public Assignment partition(List<String> clients, List<String> hosts, long seed) {
        List<String> clientsCopy = Lists.newArrayList(clients);
        List<String> hostsCopy = Lists.newArrayList(hosts);
        shuffle(clientsCopy, hostsCopy, seed);

        int numHosts = hosts.size();

        Assignment.Builder builder = Assignment.builder();
        int index = 0;
        for (String client : clients) {
            for (int i = 0; i < miniclusterSize; i++, index++) {
                builder.addMapping(client, hosts.get(index % numHosts));
            }
        }
        return builder.build();
    }

    private void shuffle(List<String> clientsCopy, List<String> hostsCopy, long seed) {
        Random random = new Random(seed);
        Collections.shuffle(clientsCopy, random);
        Collections.shuffle(hostsCopy, random);
    }
}
