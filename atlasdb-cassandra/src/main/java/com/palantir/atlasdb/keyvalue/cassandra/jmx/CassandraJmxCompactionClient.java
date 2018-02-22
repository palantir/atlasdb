/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.management.remote.JMXConnector;

import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * Maintains a JMX client for each node in C* cluster.
 */
public class CassandraJmxCompactionClient {
    private static final Logger log = LoggerFactory.getLogger(CassandraJmxCompactionClient.class);
    private final String host;
    private final int port;
    private final JMXConnector jmxConnector;
    private final StorageServiceMBean storageServiceProxy;
    private final HintedHandOffManagerMBean hintedHandoffProxy;
    private final CompactionManagerMBean compactionManagerProxy;

    protected CassandraJmxCompactionClient(
            String host,
            int port,
            JMXConnector jmxConnector,
            StorageServiceMBean storageServiceProxy,
            HintedHandOffManagerMBean hintedHandoffProxy,
            CompactionManagerMBean compactionManagerProxy) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(host));
        Preconditions.checkArgument(port > 0);
        this.host = host;
        this.port = port;
        this.jmxConnector = Preconditions.checkNotNull(jmxConnector, "jmxConnector cannot be null");
        this.storageServiceProxy = Preconditions.checkNotNull(storageServiceProxy,
                "storageServiceProxy cannot be null");
        this.hintedHandoffProxy = Preconditions.checkNotNull(hintedHandoffProxy, "hintedHandoffProxy cannot be null");
        this.compactionManagerProxy = Preconditions.checkNotNull(compactionManagerProxy,
                "compactionManagerProxy cannot be null");
    }

    public static CassandraJmxCompactionClient create(String host,
                                                      int port,
                                                      JMXConnector jmxConnector,
                                                      StorageServiceMBean storageServiceProxy,
                                                      HintedHandOffManagerMBean hintedHandoffProxy,
                                                      CompactionManagerMBean compactionManagerProxy) {
        return new CassandraJmxCompactionClient(
                host,
                port,
                jmxConnector,
                storageServiceProxy,
                hintedHandoffProxy,
                compactionManagerProxy);
    }

    public void deleteLocalHints() {
        // hintedHandoff needs to be deleted to make sure data will not resurrect.
        hintedHandoffProxy.deleteHintsForEndpoint(host);
    }

    public void forceTableFlush(String keyspace, TableReference tableRef) {
        log.trace("flushing {}.{} for tombstone compaction!", keyspace, tableRef.getQualifiedName());
        try {
            storageServiceProxy.forceKeyspaceFlush(keyspace, tableRef.getQualifiedName());
        } catch (Exception e) {
            log.error("Failed to flush table {}.{}.", keyspace, tableRef.getQualifiedName(), e);
            Throwables.propagateIfPossible(e);
        }
    }

    private static final int RETRY_TIMES = 3;
    private static final long RETRY_INTERVAL_IN_SECONDS = 5;

    /**
     * retry for a number of times. The reason why we need to retry compaction is:
     * during a node's coming back process, the keyspace is not ready to be compacted
     * even the JMX connection can be established.
     */
    public boolean forceTableCompaction(String keyspace, TableReference tableRef) {
        boolean status = tryTableCompactionInternal(keyspace, tableRef);
        int retries = 0;
        while (!status && retries < RETRY_TIMES) {
            retries++;
            log.info("Failed to compact, retrying in {} seconds for {} time(s)", RETRY_INTERVAL_IN_SECONDS, retries);
            try {
                TimeUnit.SECONDS.sleep(RETRY_INTERVAL_IN_SECONDS);
            } catch (InterruptedException e) {
                log.error("Sleep interrupted while trying to compact", e);
            }
            status = tryTableCompactionInternal(keyspace, tableRef);
        }
        if (!status) {
            log.error("Failed to compact after {} retries.", RETRY_INTERVAL_IN_SECONDS);
        }
        return status;
    }

    private boolean tryTableCompactionInternal(String keyspace, TableReference tableRef) {
        try {
            Stopwatch stopWatch = Stopwatch.createStarted();
            storageServiceProxy.forceKeyspaceCompaction(true, keyspace, tableRef.getQualifiedName());
            log.info("Compaction for {}.{} completed in {}", keyspace, tableRef, stopWatch.stop());
        } catch (Exception e) {
            log.error("Failed to compact {}.{}.", keyspace, tableRef, e);
            Throwables.propagateIfPossible(e);
            return false;
        }
        return true;
    }

    public List<String> getCompactionStatus() {
        return compactionManagerProxy.getCompactionSummary();
    }

    public void close() {
        if (jmxConnector != null) {
            try {
                jmxConnector.close();
            } catch (IOException e) {
                log.error("Error in closing Cassandra JMX compaction connection.", e);
            }
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CassandraJmxCompactionClient that = (CassandraJmxCompactionClient) obj;
        return port == that.port
                && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("host", host)
                .add("port", port)
                .toString();
    }
}
