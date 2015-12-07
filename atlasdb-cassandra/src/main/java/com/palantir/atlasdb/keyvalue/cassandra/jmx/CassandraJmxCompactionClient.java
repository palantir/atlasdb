/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra.jmx;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.common.base.Throwables;

/**
 * Maintains a JMX client for each node in C* cluster
 */
public class CassandraJmxCompactionClient {
    private static final Logger log = LoggerFactory.getLogger(CassandraJmxCompactionClient.class);
    private static final int RETRY_TIMES = 3;
    private static final long RETRY_INTERVAL_IN_SECONDS = 5;

    private final String host;
    private final int port;
    private final String username;
    private final String password;

    private JMXConnector jmxConnector;
    private CompactionManagerMBean compactionManagerProxy;
    private StorageServiceMBean storageServiceProxy;
    private HintedHandOffManagerMBean hintedHandoffProxy;

    private CassandraJmxCompactionClient(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.username = builder.username;
        this.password = builder.password;
    }

    public static class Builder {
        private String host;
        private int port;
        private String username = "";
        private String password = "";

        public Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public Builder username(String val) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(val), "username:[%s] should not be empty or null.", val);
            this.username = val;
            return this;
        }

        public Builder password(String val) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(val), "password:[%s] should not be empty or null.", val);
            this.password = val;
            return this;
        }

        public Optional<CassandraJmxCompactionClient> build() {
            CassandraJmxCompactionClient client = new CassandraJmxCompactionClient(this);
            boolean status = client.connect();
            int retries = 0;
            while (status == false && retries < RETRY_TIMES) {
                retries++;
                log.info("Failed to connect to JMX, retrying in {} seconds for {} time(s)", RETRY_INTERVAL_IN_SECONDS, retries);
                try {
                    TimeUnit.SECONDS.sleep(RETRY_INTERVAL_IN_SECONDS);
                } catch (InterruptedException e) {
                    log.error("Sleep interupted while trying to connect to JMX server", e);
                }
                status = client.connect();
            }
            if (status == false) {
                log.error("Failed to connect to JMX after {} retries on node {}:{}!", RETRY_TIMES, host, port);
                return Optional.absent();
            }
            return Optional.of(client);
        }
    }

    /**
     * Create a connection to the JMX agent and setup the M[X]Bean proxies.
     */
    private boolean connect() {
        Map<String, Object> env = new HashMap<String, Object>();
        String[] creds = { username, password };
        env.put(JMXConnector.CREDENTIALS, creds);

        JMXServiceURL jmxServiceUrl = null;
        MBeanServerConnection mbeanServerConn = null;
        try {
            jmxServiceUrl = new JMXServiceURL(String.format(CassandraConstants.JMX_RMI, host, port));
            jmxConnector = JMXConnectorFactory.connect(jmxServiceUrl, env);
            mbeanServerConn = jmxConnector.getMBeanServerConnection();

            ObjectName name = new ObjectName(CassandraConstants.STORAGE_SERVICE_OBJECT_NAME);
            storageServiceProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);

            name = new ObjectName(CassandraConstants.COMPACTION_MANAGER_OBJECT_NAME);
            compactionManagerProxy = JMX.newMBeanProxy(mbeanServerConn, name, CompactionManagerMBean.class);

            name = new ObjectName(CassandraConstants.HINTED_HANDOFF_MANAGER_OBJECT_NAME);
            hintedHandoffProxy = JMX.newMBeanProxy(mbeanServerConn, name, HintedHandOffManagerMBean.class);
        } catch (MalformedURLException e) {
            log.error("Wrong JMX service URL.", e);
            return false;
        } catch (IOException e) {
            log.error("JMXConnectorFactory cannot connect to the JMX service URL.", e);
            return false;
        } catch (MalformedObjectNameException e) {
            log.error("JMX ObjectName has wrong fomat.", e);
            return false;
        }
        return true;
    }

    public void close() {
        try {
            if (jmxConnector != null) {
                jmxConnector.close();
            }
        } catch (IOException e) {
            log.error("Error in closing Cassandra JMX compaction connection.", e);
        }
    }

    public void deleteLocalHints() {
        // hintedHandoff needs to be deleted to make sure data will not reappear later
        hintedHandoffProxy.deleteHintsForEndpoint(host);
        log.trace("hintedhandoff on node {}:{} was deleted for tombstone compaction.", host, port);
    }

    public void forceTableFlush(String keyspace, String tableName) {
        log.trace("flushing {}.{} on node {}:{} for tombstone compaction!", keyspace, tableName, host, port);
        try {
            storageServiceProxy.forceKeyspaceFlush(keyspace, tableName);
        } catch (IOException e) {
            log.error("forceTableFlush IOException: {}", e.getMessage());
            Throwables.throwUncheckedException(e);
        } catch (ExecutionException e) {
            log.error("forceTableFlush ExecutionException: {}", e.getMessage());
            Throwables.throwUncheckedException(e);
        } catch (InterruptedException e) {
            log.error("forceTableFlush InterruptedException: {}", e.getMessage());
            Throwables.throwUncheckedException(e);
        }
    }

    /**
     * retry for a number of times. The reason why we need to retry compaction is:
     * during a node's coming back process, the keyspace is not ready to be compacted
     * even the JMX connection can be established.
     * @param keyspace
     * @param tableName
     */
    public boolean forceTableCompaction(String keyspace, String tableName) {
        boolean status = tryTableCompaction(keyspace, tableName);
        int retries = 0;
        while (status == false && retries < RETRY_TIMES) {
            retries++;
            log.info("Failed to compact, retrying in {} seconds for {} time(s) on node {}:{}", RETRY_INTERVAL_IN_SECONDS, retries, host, port);
            try {
                TimeUnit.SECONDS.sleep(RETRY_INTERVAL_IN_SECONDS);
            } catch (InterruptedException e) {
                log.error("Sleep interupted while trying to compact", e);
            }
            status = tryTableCompaction(keyspace, tableName);
        }
        if (status == false) {
            log.error("Failed to compact after {} retries on node {}:{}.", RETRY_INTERVAL_IN_SECONDS, host, port);
        }
        return status;
    }

    private boolean tryTableCompaction(String keyspace, String tableName) {
        try {
            Stopwatch stopWatch = Stopwatch.createStarted();
            storageServiceProxy.forceKeyspaceCompaction(true, keyspace, tableName);
            log.info("Compaction for {}.{} completed on node {}:{} in {}", keyspace, Arrays.asList(tableName), host, port, stopWatch.stop());
        } catch (IOException e) {
            log.error("Invalid keyspace or tableNames specified for forceTableCompaction()", e);
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

    public List<Map<String, String>> getCompactionStatus() {
        return compactionManagerProxy.getCompactions();
    }

    public List<String> getLiveNodes() {
        return storageServiceProxy.getLiveNodes();
    }

    public String getHost() {
        return host;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof CassandraJmxCompactionClient)) {
            return false;
        }

        CassandraJmxCompactionClient rhs = (CassandraJmxCompactionClient) obj;
        return Objects.equal(this.host, rhs.host) && this.port == rhs.port;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("host", host).add("port", port).toString();
    }
}
