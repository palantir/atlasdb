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

package com.palantir.atlasdb.keyvalue.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Mutation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;

public class MutationMap {
    private final Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap;

    public MutationMap() {
        this.mutationMap = Maps.newHashMap();
    }

    public void addMutationForRow(ByteBuffer rowName, TableReference tableRef, Mutation mutation) {
        Map<String, List<Mutation>> rowPuts = mutationMap.computeIfAbsent(rowName, row -> Maps.newHashMap());

        List<Mutation> tableMutations = rowPuts.computeIfAbsent(
                AbstractKeyValueService.internalTableName(tableRef),
                k -> Lists.newArrayList());

        tableMutations.add(mutation);
    }

    public Map<ByteBuffer, Map<String, List<Mutation>>> get() {
        return mutationMap;
    }
}
