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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.pooling.PoolingContainer;

public class CassandraClientPoolingContainer implements PoolingContainer<Client> {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolingContainer.class);

    private final String host;
    private final String keyspace;
    private final int port;
    private final boolean isSsl;
    private final int socketTimeoutMillis;
    private final int socketQueryTimeoutMillis;
    private final AtomicLong count = new AtomicLong();
    private final GenericObjectPool<Client> clientPool;

    private CassandraClientPoolingContainer(Builder builder){
        this.host = builder.host;
        this.port = builder.port;
        this.isSsl = builder.isSsl;
        this.keyspace = builder.keyspace;
        this.socketTimeoutMillis = builder.socketTimeoutMillis;
        this.socketQueryTimeoutMillis = builder.socketQueryTimeoutMillis;
        this.clientPool = builder.clientPool;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V, K extends Exception> V runWithPooledResource(FunctionCheckedException<Client, V, K> f)
            throws K {
        final String origName = Thread.currentThread().getName();
        Thread.currentThread().setName(origName
                + " calling cassandra host " + host
                + " started at " + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(new Date())
                + " - " + count.getAndIncrement());
        try {
            return runWithGoodResource(f);
        } catch (Throwable t) {
            log.warn("Failed while connecting to host: " + host, t);
            if (t instanceof Exception) {
                throw (K) t;
            } else {
                throw (Error) t;
            }
        } finally {
            Thread.currentThread().setName(origName);
        }
    }

    @SuppressWarnings("unchecked")
    private <V, K extends Exception> V runWithGoodResource(FunctionCheckedException<Client, V, K> f)
            throws K {
        boolean shouldReuse = true;
        Client resource = null;
        try {
            resource = clientPool.borrowObject();
            return f.apply(resource);
        } catch (Exception e) {
            if (e instanceof TTransportException
                    || e instanceof TProtocolException) {
                log.warn("Not reusing resource {} due to {}", resource, e);
                shouldReuse = false;
            }
            if (e instanceof TTransportException
                    && ((TTransportException) e).getType() == TTransportException.END_OF_FILE) {
                // If we have an end of file this is most likely due to this cassandra node being bounced.
                clientPool.clear();
            }
            throw (K) e;
        } finally {
            if (resource != null) {
                if (shouldReuse) {
                    log.info("Returning {} to pool", resource);
                    clientPool.returnObject(resource);
                } else {
                    invalidateQuietly(resource);
                }
            }
        }
    }

    private void invalidateQuietly(Client resource) {
        try {
            log.info("Discarding: {}", resource);
            clientPool.invalidateObject(resource);
        } catch (Exception e) {
            // Ignore
        }
    }

    @Override
    public <V> V runWithPooledResource(Function<Client, V> f) {
        throw new UnsupportedOperationException("you should use FunctionCheckedException<?, ?, Exception> "
                + "to ensure the TTransportException type is propagated correctly.");
    }

    @Override
    public void shutdownPooling() {
        clientPool.close();
    }

    public static class Builder{
        private final String host;
        private final int port;

        // default values are provided
        private int poolSize = 20;
        private String keyspace = "atlasdb";
        private boolean isSsl = false;
        private int socketTimeoutMillis = 2000;
        private int socketQueryTimeoutMillis = 62000;

        private GenericObjectPool<Client> clientPool;

        public Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public Builder poolSize(int val){
            poolSize = val;
            return this;
        }

        public Builder keyspace(String val){
            keyspace = val;
            return this;
        }

        public Builder isSsl(boolean val){
            isSsl = val;
            return this;
        }

        public Builder socketTimeout(int val){
            socketTimeoutMillis = val;
            return this;
        }

        public Builder socketQueryTimeout(int val){
            socketQueryTimeoutMillis = val;
            return this;
        }

        private GenericObjectPool<Client> createClientPool() {
            CassandraClientFactory cassandraClientFactory =
                    new CassandraClientFactory(host,
                            keyspace,
                            port,
                            isSsl,
                            socketTimeoutMillis,
                            socketQueryTimeoutMillis);
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
            config.setMinIdle(poolSize);
            config.setTestOnBorrow(true);
            config.setBlockWhenExhausted(true);
            // TODO: Should we make these configurable/are these intelligent values?
            config.setMaxTotal(5 * poolSize);
            config.setMaxIdle(5 * poolSize);
            config.setMinEvictableIdleTimeMillis(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
            config.setTimeBetweenEvictionRunsMillis(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
            config.setNumTestsPerEvictionRun(-1); // Test all idle objects for eviction
            config.setJmxNamePrefix(host);
            return new GenericObjectPool<Client>(cassandraClientFactory, config);
        }

        public CassandraClientPoolingContainer build(){
            clientPool = createClientPool();
            return new CassandraClientPoolingContainer(this);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("host", this.host)
                .add("port", this.port)
                .add("keyspace", this.keyspace)
                .add("isSsl", this.isSsl)
                .add("socketTimeoutMillis", this.socketTimeoutMillis)
                .add("socketQueryTimeoutMillis", this.socketQueryTimeoutMillis)
                .toString();
    }
}
