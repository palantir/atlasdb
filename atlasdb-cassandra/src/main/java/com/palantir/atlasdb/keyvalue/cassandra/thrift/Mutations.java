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

import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;

public final class Mutations {

    private Mutations() { }

    public static Mutation rangeTombstoneForColumn(byte[] columnName, long maxTimestampExclusive) {
        Deletion deletion = new Deletion()
                .setTimestamp(Long.MAX_VALUE)
                .setPredicate(SlicePredicates.rangeTombstoneForColumn(columnName, maxTimestampExclusive));

        return new Mutation().setDeletion(deletion);
    }

}
