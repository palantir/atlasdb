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

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.pooling.PoolingContainer;

public class CassandraClientPoolingContainer implements PoolingContainer<Client> {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolingContainer.class);

    private final InetSocketAddress host;
    private CassandraKeyValueServiceConfig config;
    private final AtomicLong count = new AtomicLong();
    private final GenericObjectPool<Client> clientPool;

    public CassandraClientPoolingContainer(InetSocketAddress host, CassandraKeyValueServiceConfig config) {
        this.host = host;
        this.config = config;
        this.clientPool = createClientPool();
    }

    public InetSocketAddress getHost() {
        return host;
    }


    // returns negative if not available; only expected use is debugging
    protected int getPoolUtilization() {
        return clientPool.getNumActive();
    }

    // returns negative if unbounded; only expected use is debugging
    protected int getPoolSize() {
        return clientPool.getMaxTotal();
    }

    @Override
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
            log.warn("Error occurred talking to host '{}': {}", host, t.toString());
            throw t;
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
            if (isInvalidClientConnection(e)) {
                log.warn("Not reusing resource {} due to {}", resource, e.toString(), e);
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
                    log.debug("Returning {} to pool", resource);
                    eagerlyCleanupReadBuffersFromIdleConnection(resource);
                    clientPool.returnObject(resource);
                } else {
                    invalidateQuietly(resource);
                }
            }
        }
    }

    private static void eagerlyCleanupReadBuffersFromIdleConnection(Client idleClient) {
        // eagerly cleanup idle-connection read buffer to keep a smaller memory footprint
        try {
            TTransport transport = idleClient.getInputProtocol().getTransport();
            if (transport instanceof TFramedTransport) {
                Field readBuffer_ = ((TFramedTransport) transport).getClass().getDeclaredField("readBuffer_");
                readBuffer_.setAccessible(true);
                TMemoryInputTransport tMemoryInputTransport = (TMemoryInputTransport) readBuffer_.get(transport);
                byte[] underlyingBuffer = tMemoryInputTransport.getBuffer();
                if (underlyingBuffer != null) {
                    log.debug("During {} check-in, cleaned up a read buffer of {} bytes", idleClient, underlyingBuffer.length);
                    tMemoryInputTransport.clear();
                }
            }
        } catch (Exception e) {
            log.debug("Couldn't clean up read buffers on pool check-in.", e);
        }
    }

    private static boolean isInvalidClientConnection(Exception e) {
        return e instanceof TTransportException
                || e instanceof TProtocolException
                || e instanceof NoSuchElementException;
    }

    private void invalidateQuietly(Client resource) {
        try {
            log.debug("Discarding: {}", resource);
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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("host", this.host)
                .add("keyspace", config.keyspace())
                .add("isSsl", config.ssl())
                .add("socketTimeoutMillis", config.socketTimeoutMillis())
                .add("socketQueryTimeoutMillis", config.socketQueryTimeoutMillis())
                .toString();
    }

    private GenericObjectPool<Client> createClientPool() {
        CassandraClientFactory cassandraClientFactory =
                new CassandraClientFactory(host,
                        config.keyspace(),
                        config.credentials(),
                        config.ssl(),
                        config.socketTimeoutMillis(),
                        config.socketQueryTimeoutMillis());
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMinIdle(config.poolSize());
        poolConfig.setTestOnBorrow(true);
        poolConfig.setBlockWhenExhausted(true);
        // TODO: Should we make these configurable/are these intelligent values?
        poolConfig.setMaxTotal(5 * config.poolSize());
        poolConfig.setMaxIdle(5 * config.poolSize());
        poolConfig.setMinEvictableIdleTimeMillis(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
        poolConfig.setTimeBetweenEvictionRunsMillis(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
        poolConfig.setNumTestsPerEvictionRun(-1); // Test all idle objects for eviction
        poolConfig.setJmxNamePrefix(host.getHostString());
        poolConfig.setMaxWaitMillis(config.socketTimeoutMillis());
        return new GenericObjectPool<Client>(cassandraClientFactory, poolConfig);
    }
}
