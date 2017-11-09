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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

@SuppressWarnings({"all"}) // thrift variable names.
public interface CassandraClient {
    Cassandra.Client rawClient();

    Map<ByteBuffer,List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent,
            SlicePredicate predicate, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, org.apache.thrift.TException;

    List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, org.apache.thrift.TException;

    void batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, org.apache.thrift.TException;
}
