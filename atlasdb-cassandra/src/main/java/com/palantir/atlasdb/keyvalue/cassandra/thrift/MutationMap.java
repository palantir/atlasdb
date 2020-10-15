/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra.thrift;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.cassandra.thrift.Mutation;

@NotThreadSafe
public class MutationMap {
    private final Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap;

    public MutationMap() {
        this.mutationMap = Maps.newHashMap();
    }

    /**
     * Adds the given mutation of the given cell for the given tableRef.
     * This creates internal map values, if those are not already present.
     * Not thread safe - if the map values are absent, then creating two values simultaneously in different threads may
     * lead to a "last write wins" race condition.
     */
    public void addMutationForCell(Cell cell, TableReference tableRef, Mutation mutation) {
        ByteBuffer rowName = ByteBuffer.wrap(cell.getRowName());
        Map<String, List<Mutation>> rowPuts = mutationMap.computeIfAbsent(rowName, row -> Maps.newHashMap());

        List<Mutation> tableMutations = rowPuts.computeIfAbsent(
                AbstractKeyValueService.internalTableName(tableRef),
                k -> Lists.newArrayList());

        tableMutations.add(mutation);
    }

    /**
     * Gets the MutationMap's internal map object, for use by Thrift's batch_mutate API.
     *
     * @return a reference to (not a copy of) the map wrapped by the MutationMap object
     */
    public Map<ByteBuffer, Map<String, List<Mutation>>> toMap() {
        return mutationMap;
    }
}
