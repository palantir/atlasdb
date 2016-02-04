/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra.jmx;

import java.util.concurrent.Callable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class TombStoneCompactionTask implements Callable<Boolean> {
    private final CassandraJmxCompactionClient client;
    private final String keyspace;
    private final String tableName;

    TombStoneCompactionTask(CassandraJmxCompactionClient client, String keyspace, String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(keyspace));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));

        this.client = Preconditions.checkNotNull(client);
        this.keyspace = keyspace;
        this.tableName = tableName;
    }

    @Override
    public Boolean call() throws Exception {
        // make sure tombstone is persisted on disk for tombstone compaction
        client.forceTableFlush(keyspace, tableName);
        client.forceTableCompaction(keyspace, tableName);
        return true;
    }
}