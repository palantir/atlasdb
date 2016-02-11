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

import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class TombstoneCompactionTask implements Callable<Boolean> {
    private static final Logger log = LoggerFactory.getLogger(TombstoneCompactionTask.class);
    private final CassandraJmxCompactionClient client;
    private final String keyspace;
    private final String tableName;

    TombstoneCompactionTask(CassandraJmxCompactionClient client, String keyspace, String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(keyspace));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));

        this.client = Preconditions.checkNotNull(client);
        this.keyspace = keyspace;
        this.tableName = tableName;
    }

    @Override
    public Boolean call() {
        try {
            // table flush will make sure tombstone is persisted on disk for tombstone compaction
            client.forceTableFlush(keyspace, tableName);
            client.forceTableCompaction(keyspace, tableName);
        } catch (Exception e) {
            if (e instanceof UndeclaredThrowableException) {
                log.error("Major LCS compactions are only supported against C* 2.2+; " +
                        "you will need to manually re-arrange SSTables into L0 " +
                        "if you want all deleted data immediately removed from the cluster.", e);
                return false;
            }
            log.error("Failed to complete TombstoneCompactionTask.", e);
            return false;
        }
        return true;
    }
}
