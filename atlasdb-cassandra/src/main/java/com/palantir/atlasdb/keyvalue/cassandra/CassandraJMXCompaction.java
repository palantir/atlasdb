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
package com.palantir.atlasdb.keyvalue.cassandra;

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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.palantir.common.base.Throwables;

/**
 * Maintains a JMX connection for a C* node
 */
public class CassandraJMXCompaction {
    private static final Logger log = LoggerFactory.getLogger(CassandraJMXCompaction.class);
    private static final int RETRY_TIMES = 3;
    private static final long RETRY_INTERVAL_IN_SECONDS = 5;

    private final String host;
    private final int port;
    private final String username;
    private final String password;

    private JMXConnector jmxc;
    private CompactionManagerMBean compactionProxy;
    private StorageServiceMBean ssProxy;
    private HintedHandOffManagerMBean hintedHandoffProxy;

    private CassandraJMXCompaction(Builder builder) {
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
            Preconditions.checkArgument(!Strings.isNullOrEmpty(host));
            Preconditions.checkArgument(port > 0, "invalid port:[%s], must be greater than zero", port);
            this.host = host;
            this.port = port;
        }

        public Builder username(String val) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(val), "username:[%s] should not be empty or null", val);
            this.username = val;
            return this;
        }

        public Builder password(String val) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(val), "password:[%s] should not be empty or null", val);
            this.password = val;
            return this;
        }

        public CassandraJMXCompaction build() {
            CassandraJMXCompaction compaction = new CassandraJMXCompaction(this);
            boolean status = compaction.connect();
            int retries = 0;
            while (status == false && retries < RETRY_TIMES) {
                retries++;
                log.info("Failed to connect to JMX, retrying in {} seconds for {} time(s)", RETRY_INTERVAL_IN_SECONDS, retries);
                try {
                    TimeUnit.SECONDS.sleep(RETRY_INTERVAL_IN_SECONDS);
                } catch (InterruptedException e) {
                    log.error("Sleep interupted while trying to connect to JMX server", e);
                }
                status = compaction.connect();
            }
            if (status == false) {
                log.error("Failed to connect to JMX after {} retries on node {}!", RETRY_TIMES, host);
                return null;
            }
            return compaction;
        }
    }

    /**
     * Create a connection to the JMX agent and setup the M[X]Bean proxies.
     */
    private boolean connect() {
        Map<String, Object> env = new HashMap<String, Object>();
        String[] creds = { username, password };
        env.put(JMXConnector.CREDENTIALS, creds);

        JMXServiceURL jmxUrl = null;
        MBeanServerConnection mbeanServerConn = null;
        try {
            jmxUrl = new JMXServiceURL(String.format(CassandraConstants.JMX_RMI, host, port));
            jmxc = JMXConnectorFactory.connect(jmxUrl, env);
            mbeanServerConn = jmxc.getMBeanServerConnection();

            ObjectName name = new ObjectName(CassandraConstants.STORAGE_SERVICE_OBJECT_NAME);
            ssProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);

            name = new ObjectName(CassandraConstants.COMPACTION_MANAGER_OBJECT_NAME);
            compactionProxy = JMX.newMBeanProxy(mbeanServerConn, name, CompactionManagerMBean.class);

            name = new ObjectName(CassandraConstants.HINTED_HANDOFF_MANAGER_OBJECT_NAME);
            hintedHandoffProxy = JMX.newMBeanProxy(mbeanServerConn, name, HintedHandOffManagerMBean.class);
        } catch (MalformedURLException e) {
            log.error("Wrong JMX service URL", e);
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
            if (jmxc != null) {
                jmxc.close();
            }
        } catch (IOException e) {
            log.error("Error in closing Cassandra JMX compaction connection.", e);
        }
    }

    public void deleteLocalHints() {
        // hintedHandoff needs to be deleted to make sure data will not reappear later
        hintedHandoffProxy.deleteHintsForEndpoint(host);
        log.trace("hintedhandoff on host:{} was deleted for tombstone compaction!", host);
    }

    public void forceTableFlush(String keyspace, String tableName) {
        log.trace("flushing {}.{} on host:{} for tombstone compaction!", keyspace, tableName, host);
        try {
            ssProxy.forceKeyspaceFlush(keyspace, tableName);
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
        boolean status = performTableCompaction(keyspace, tableName);
        int retries = 0;
        while (status == false && retries < RETRY_TIMES) {
            retries++;
            log.info("Failed to compact, retrying in {} seconds for {} time(s) on node {}", RETRY_INTERVAL_IN_SECONDS, retries, host);
            try {
                TimeUnit.SECONDS.sleep(RETRY_INTERVAL_IN_SECONDS);
            } catch (InterruptedException e) {
                log.error("Sleep interupted while trying to compact", e);
            }
            status = performTableCompaction(keyspace, tableName);
        }
        if (status == false) {
            log.error("Failed to compact after {} retries on node {}!", RETRY_INTERVAL_IN_SECONDS, host);
        }
        return status;
    }

    private boolean performTableCompaction(String keyspace, String tableName) {
        try {
            Stopwatch timer = Stopwatch.createStarted();
            ssProxy.forceKeyspaceCompaction(keyspace, tableName);
            log.info("Compaction for {}:{} completed on node {} in {}", keyspace, Arrays.asList(tableName), host, timer.stop());
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

    public int getPendingTasks() {
        return compactionProxy.getPendingTasks();
    }

    public List<String> getLiveNodes() {
        return ssProxy.getLiveNodes();
    }

    public String getHost() {
        return host;
    }

    /* auto generated by eclipse */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        return result;
    }

    /* auto generated by eclipse */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CassandraJMXCompaction other = (CassandraJMXCompaction) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (port != other.port)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "CassandraJMXCompaction [host=" + host + ", port=" + port + "]";
    }
}
