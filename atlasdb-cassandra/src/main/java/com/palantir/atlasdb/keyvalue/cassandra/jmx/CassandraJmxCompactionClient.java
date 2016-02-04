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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.management.remote.JMXConnector;

import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

/**
 * Maintains a JMX client for each node in C* cluster
 */
public class CassandraJmxCompactionClient {
    private static final Logger log = LoggerFactory.getLogger(CassandraJmxCompactionClient.class);
    private final String host;
    private final int port;
    private final JMXConnector jmxConnector;
    private final StorageServiceMBean storageServiceProxy;
    private final HintedHandOffManagerMBean hintedHandoffProxy;
    private final CompactionManagerMBean compactionManagerProxy;

    private CassandraJmxCompactionClient(String host,
                                         int port,
                                         JMXConnector jmxConnector,
                                         StorageServiceMBean storageServiceProxy,
                                         HintedHandOffManagerMBean hintedHandoffProxy,
                                         CompactionManagerMBean compactionManagerProxy) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(host));
        Preconditions.checkArgument(port > 0);
        this.host = host;
        this.port = port;
        this.jmxConnector = Preconditions.checkNotNull(jmxConnector);
        this.storageServiceProxy = Preconditions.checkNotNull(storageServiceProxy);
        this.hintedHandoffProxy = Preconditions.checkNotNull(hintedHandoffProxy);
        this.compactionManagerProxy = Preconditions.checkNotNull(compactionManagerProxy);
    }

    public static CassandraJmxCompactionClient create(String host,
                                                      int port,
                                                      JMXConnector jmxConnector,
                                                      StorageServiceMBean storageServiceProxy,
                                                      HintedHandOffManagerMBean hintedHandoffProxy,
                                                      CompactionManagerMBean compactionManagerProxy) {
        return new CassandraJmxCompactionClient(host, port, jmxConnector, storageServiceProxy, hintedHandoffProxy, compactionManagerProxy);
    }

    public void truncateAllHints() throws ExecutionException, InterruptedException {
        // hintedHandoff needs to be deleted to make sure data will resurrect.
        hintedHandoffProxy.truncateAllHints();
    }

    public void forceTableFlush(String keyspace, String tableName) {
        log.trace("flushing {}.{} for tombstone compaction!", keyspace, tableName);
        try {
            storageServiceProxy.forceKeyspaceFlush(keyspace, tableName);
        } catch (IOException e) {
            log.error("forceTableFlush IOException: {}", e.getMessage());
            Throwables.propagate(e);
        } catch (ExecutionException e) {
            log.error("forceTableFlush ExecutionException: {}", e.getMessage());
            Throwables.propagate(e);
        } catch (InterruptedException e) {
            log.error("forceTableFlush InterruptedException: {}", e.getMessage());
            Throwables.propagate(e);
        }
    }

    private static final int RETRY_TIMES = 3;
    private static final long RETRY_INTERVAL_IN_SECONDS = 5;

    /**
     * retry for a number of times. The reason why we need to retry compaction is:
     * during a node's coming back process, the keyspace is not ready to be compacted
     * even the JMX connection can be established.
     */
    public boolean forceTableCompaction(String keyspace, String tableName) {
        boolean status = tryTableCompactionInternal(keyspace, tableName);
        int retries = 0;
        while (status == false && retries < RETRY_TIMES) {
            retries++;
            log.info("Failed to compact, retrying in {} seconds for {} time(s)", RETRY_INTERVAL_IN_SECONDS, retries);
            try {
                TimeUnit.SECONDS.sleep(RETRY_INTERVAL_IN_SECONDS);
            } catch (InterruptedException e) {
                log.error("Sleep interrupted while trying to compact", e);
            }
            status = tryTableCompactionInternal(keyspace, tableName);
        }
        if (status == false) {
            log.error("Failed to compact after {} retries.", RETRY_INTERVAL_IN_SECONDS);
        }
        return status;
    }

    private boolean tryTableCompactionInternal(String keyspace, String tableName) {
        try {
            Stopwatch stopWatch = Stopwatch.createStarted();
            storageServiceProxy.forceKeyspaceCompaction(true, keyspace, tableName);
            log.info("Compaction for {}.{} completed in {}", keyspace, tableName, stopWatch.stop());
        } catch (IOException e) {
            log.error("Invalid keyspace or tableName specified for forceTableCompaction()", e);
            return false;
        } catch (ExecutionException e) {
            log.error("ExecutionException in forceTableCompaction()", e);
            return false;
        } catch (InterruptedException e) {
            log.error("InterruptedException in forceTableCompaction()", e);
            return false;
        }
        return true;
    }

    public List<String> getCompactionStatus() {
        return compactionManagerProxy.getCompactionSummary();
    }

    public void close() {
        if (jmxConnector == null) {
            return;
        }

        try {
            jmxConnector.close();
        } catch (IOException e) {
            log.error("Error in closing Cassandra JMX compaction connection.", e);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CassandraJmxCompactionClient)) {
            return false;
        }
        CassandraJmxCompactionClient rhs = (CassandraJmxCompactionClient) obj;
        return Objects.equal(this.host, rhs.host) && Objects.equal(this.port, rhs.port);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("host", host)
                .add("port", port)
                .toString();
    }
}
