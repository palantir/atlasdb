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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final AtomicInteger openRequests = new AtomicInteger();
    private final GenericObjectPool<Client> clientPool;

    public CassandraClientPoolingContainer(InetSocketAddress host, CassandraKeyValueServiceConfig config) {
        this.host = host;
        this.config = config;
        this.clientPool = createClientPool();
    }

    public InetSocketAddress getHost() {
        return host;
    }

    /**
     * Number of open requests to {@link #runWithPooledResource(FunctionCheckedException)}.
     * This is different from the number of active objects in the pool, as creating a new
     * pooled object can block on {@link CassandraClientFactory#create()}} before being added
     * to the client pool.
     */
    protected int getOpenRequests() {
        return openRequests.get();
    }

    // returns negative if not available; only expected use is debugging
    protected int getActiveCheckouts() {
        return clientPool.getNumActive();
    }

    // returns negative if unbounded; only expected use is debugging
    protected int getPoolSize() {
        return clientPool.getMaxTotal();
    }

    @Override
    public <V, K extends Exception> V runWithPooledResource(FunctionCheckedException<Client, V, K> fn)
            throws K {
        final String origName = Thread.currentThread().getName();
        Thread.currentThread().setName(origName
                + " calling cassandra host " + host
                + " started at " + DateTimeFormatter.ISO_INSTANT.format(Instant.now())
                + " - " + count.getAndIncrement());
        try {
<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56
            openRequests.getAndIncrement();
=======
>>>>>>> merge develop into perf cli branch (#820)
            return runWithGoodResource(fn);
        } catch (Throwable t) {
            log.warn("Error occurred talking to host '{}': {}", host, t.toString());
            throw t;
        } finally {
            openRequests.getAndDecrement();
            Thread.currentThread().setName(origName);
        }
    }

    @Override
    public <V> V runWithPooledResource(Function<Client, V> fn) {
        throw new UnsupportedOperationException("you should use FunctionCheckedException<?, ?, Exception> "
                + "to ensure the TTransportException type is propagated correctly.");
    }

    @SuppressWarnings("unchecked")
    private <V, K extends Exception> V runWithGoodResource(FunctionCheckedException<Client, V, K> fn)
            throws K {
        boolean shouldReuse = true;
        Client resource = null;
        try {
            resource = clientPool.borrowObject();
            return fn.apply(resource);
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
                Field readBuffer = ((TFramedTransport) transport).getClass().getDeclaredField("readBuffer_");
                readBuffer.setAccessible(true);
                TMemoryInputTransport memoryInputTransport = (TMemoryInputTransport) readBuffer.get(transport);
                byte[] underlyingBuffer = memoryInputTransport.getBuffer();
                if (underlyingBuffer != null) {
                    log.debug("During {} check-in, cleaned up a read buffer of {} bytes",
                            idleClient, underlyingBuffer.length);
                    memoryInputTransport.clear();
                }
            }
        } catch (Exception e) {
            log.debug("Couldn't clean up read buffers on pool check-in.", e);
        }
    }

    private static boolean isInvalidClientConnection(Exception ex) {
        return ex instanceof TTransportException
                || ex instanceof TProtocolException
                || ex instanceof NoSuchElementException;
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
    public void shutdownPooling() {
        clientPool.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("host", this.host)
                .add("keyspace", config.keyspace())
                .add("usingSsl", config.usingSsl())
                .add("sslConfiguration", config.sslConfiguration().isPresent()
                        ? config.sslConfiguration().get()
                        : "unspecified")
                .add("socketTimeoutMillis", config.socketTimeoutMillis())
                .add("socketQueryTimeoutMillis", config.socketQueryTimeoutMillis())
                .toString();
    }

    /**
     * Pool size:
     *    Always keep {@code config.poolSize()} (default 20) connections around, per host.
     *    Allow bursting up to poolSize * 5 (default 100) connections per host under load.
     *
     * Borrowing from pool:
     *    On borrow, check if the connection is actually open. If it is not,
     *       immediately discard this connection from the pool, and try to take another.
     *    Borrow attempts against a fully in-use pool immediately throw a NoSuchElementException.
     *       {@code CassandraClientPool} when it sees this will:
     *          Follow an exponential backoff as a method of back pressure.
     *          Try 3 times against this host, and then give up and try against different hosts 3 additional times.
     *
     *
     * In an asynchronous thread:
     *    Every approximately 1 minute, examine approximately a third of the connections in pool.
     *    Discard any connections in this third of the pool whose TCP connections are closed.
     *    Discard any connections in this third of the pool that have been idle for more than 3 minutes,
     *       while still keeping a minimum number of idle connections around for fast borrows.
     *
     */
    private GenericObjectPool<Client> createClientPool() {
        CassandraClientFactory cassandraClientFactory = new CassandraClientFactory(host, config);
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

        poolConfig.setMinIdle(config.poolSize());
        poolConfig.setMaxIdle(5 * config.poolSize());
        poolConfig.setMaxTotal(5 * config.poolSize());

        // immediately throw when we try and borrow from a full pool; dealt with at higher level
        poolConfig.setBlockWhenExhausted(false);
        poolConfig.setMaxWaitMillis(config.socketTimeoutMillis());

        // this test is free/just checks a boolean and does not block; borrow is still fast
        poolConfig.setTestOnBorrow(true);

        poolConfig.setMinEvictableIdleTimeMillis(TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES));
        // the randomness here is to prevent all of the pools for all of the hosts
        // evicting all at at once, which isn't great for C*.
        poolConfig.setTimeBetweenEvictionRunsMillis(
                TimeUnit.MILLISECONDS.convert(60 + ThreadLocalRandom.current().nextInt(10), TimeUnit.SECONDS));
        // test one third of objects per eviction run  // (Apache Commons Pool has the worst API)
        poolConfig.setNumTestsPerEvictionRun(-3);
        poolConfig.setTestWhileIdle(true);

        poolConfig.setJmxNamePrefix(host.getHostString());

        return new GenericObjectPool<>(cassandraClientFactory, poolConfig);
    }
}
