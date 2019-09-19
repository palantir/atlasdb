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

import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import java.util.function.ToLongFunction;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;

public final class Mutations {

    private Mutations() { }

    public static Mutation fromTimestampRangeDelete(
            byte[] columnName,
            TimestampRangeDelete delete,
            long rangeTombstoneCassandraTimestamp,
            ToLongFunction<TimestampRangeDelete> exclusiveTimestampExtractor) {
        Deletion deletion = new Deletion()
                .setTimestamp(rangeTombstoneCassandraTimestamp)
                .setPredicate(getSlicePredicate(columnName, delete, exclusiveTimestampExtractor.applyAsLong(delete)));

        return new Mutation().setDeletion(deletion);
    }

    private static SlicePredicate getSlicePredicate(
            byte[] columnName, TimestampRangeDelete delete, long maxExclusiveTimestamp) {
        return delete.deleteSentinels()
                ? SlicePredicates.rangeTombstoneIncludingSentinelForColumn(columnName, maxExclusiveTimestamp)
                : SlicePredicates.rangeTombstoneForColumn(columnName, maxExclusiveTimestamp);
    }
}
