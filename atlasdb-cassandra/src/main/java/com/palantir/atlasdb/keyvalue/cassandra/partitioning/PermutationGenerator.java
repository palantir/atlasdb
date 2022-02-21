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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class PermutationGenerator {
    public static List<List<Integer>> generatePartitions(int rf, int hosts) {
        List<List<Integer>> tt = new ArrayList<>();
        tt.add(new ArrayList<>());
        List<List<Integer>> allPerms = partitionGenerator(hosts, rf, 0, tt);
        return allPerms.stream().filter(x -> x.size() == rf).collect(Collectors.toList());
    }

    private static List<List<Integer>> partitionGenerator(int num, int rf, int idx, List<List<Integer>> answer) {
        if (idx == num) {
            return answer;
        }

        int size = answer.size();

        for (int i = 0; i < size; i++) {
            List<Integer> temp = new ArrayList<>(answer.get(i));
            if (temp.size() < rf) {
                temp.add(idx);
                answer.add(temp);
            }
        }

        return partitionGenerator(num, rf, idx + 1, answer);
    }
}
