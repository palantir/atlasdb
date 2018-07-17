/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.pooling.PoolingContainer;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class CassandraClientPoolingContainer implements PoolingContainer<CassandraClient> {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolingContainer.class);

    private final QosClient qosClient;
    private final InetSocketAddress host;
    private final CassandraKeyValueServiceConfig config;
    private final MetricsManager metricsManager;
    private final AtomicLong count = new AtomicLong();
    private final AtomicInteger openRequests = new AtomicInteger();
    private final GenericObjectPool<CassandraClient> clientPool;
    private final int poolNumber;

    public CassandraClientPoolingContainer(
            MetricsManager metricsManager,
            QosClient qosClient,
            InetSocketAddress host,
            CassandraKeyValueServiceConfig config,
            int poolNumber) {
        this.metricsManager = metricsManager;
        this.qosClient = qosClient;
        this.host = host;
        this.config = config;
        this.poolNumber = poolNumber;
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
    public int getOpenRequests() {
        return openRequests.get();
    }

    // returns negative if not available; only expected use is debugging
    public int getActiveCheckouts() {
        return clientPool.getNumActive();
    }

    // returns negative if unbounded; only expected use is debugging
    public int getPoolSize() {
        return clientPool.getMaxTotal();
    }

    @Override
    public <V, K extends Exception> V runWithPooledResource(FunctionCheckedException<CassandraClient, V, K> fn)
            throws K {
        final String origName = Thread.currentThread().getName();
        Thread.currentThread().setName(origName
                + " calling cassandra host " + host
                + " started at " + DateTimeFormatter.ISO_INSTANT.format(Instant.now())
                + " - " + count.getAndIncrement());
        try {
            openRequests.getAndIncrement();
            return runWithGoodResource(fn);
        } catch (Throwable t) {
            log.warn("Error occurred talking to host '{}': {}",
                    SafeArg.of("host", CassandraLogHelper.host(host)), UnsafeArg.of("exception", t.toString()));
            throw t;
        } finally {
            openRequests.getAndDecrement();
            Thread.currentThread().setName(origName);
        }
    }

    @Override
    public <V> V runWithPooledResource(Function<CassandraClient, V> fn) {
        throw new UnsupportedOperationException("you should use FunctionCheckedException<?, ?, Exception> "
                + "to ensure the TTransportException type is propagated correctly.");
    }

    @SuppressWarnings("unchecked")
    private <V, K extends Exception> V runWithGoodResource(FunctionCheckedException<CassandraClient, V, K> fn)
            throws K {
        boolean shouldReuse = true;
        CassandraClient resource = null;
        try {
            resource = clientPool.borrowObject();
            return fn.apply(resource);
        } catch (Exception e) {
            if (isInvalidClientConnection(resource)) {
                log.warn("Not reusing resource {} due to {} of host {}",
                        UnsafeArg.of("resource", resource),
                        UnsafeArg.of("exception", e.toString()),
                        SafeArg.of("host", CassandraLogHelper.host(host)), e);
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
                    log.debug("Returning {} to pool of host {}",
                            UnsafeArg.of("resource", resource),
                            SafeArg.of("host", CassandraLogHelper.host(host)));
                    eagerlyCleanupReadBuffersFromIdleConnection(resource, host);
                    clientPool.returnObject(resource);
                } else {
                    invalidateQuietly(resource);
                }
            }
        }
    }

    private static void eagerlyCleanupReadBuffersFromIdleConnection(CassandraClient idleClient,
            InetSocketAddress host) {
        // eagerly cleanup idle-connection read buffer to keep a smaller memory footprint
        try {
            TTransport transport = idleClient.getInputProtocol().getTransport();
            if (transport instanceof TFramedTransport) {
                Field readBuffer = ((TFramedTransport) transport).getClass().getDeclaredField("readBuffer_");
                readBuffer.setAccessible(true);
                TMemoryInputTransport memoryInputTransport = (TMemoryInputTransport) readBuffer.get(transport);
                byte[] underlyingBuffer = memoryInputTransport.getBuffer();
                if (underlyingBuffer != null && memoryInputTransport.getBytesRemainingInBuffer() == 0) {
                    log.debug("During {} check-in, cleaned up a read buffer of {} bytes of host {}",
                            UnsafeArg.of("pool", idleClient),
                            SafeArg.of("bufferLength", underlyingBuffer.length),
                            SafeArg.of("host", CassandraLogHelper.host(host)));
                    memoryInputTransport.reset(PtBytes.EMPTY_BYTE_ARRAY);
                }
            }
        } catch (Exception e) {
            log.debug("Couldn't clean up read buffers on pool check-in.", e);
        }
    }

    private static boolean isInvalidClientConnection(CassandraClient client) {
        return client != null && client.isValid();
    }

    private void invalidateQuietly(CassandraClient resource) {
        try {
            log.debug("Discarding {} of host {}",
                    UnsafeArg.of("pool", resource),
                    SafeArg.of("host", CassandraLogHelper.host(host)));
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
                .add("keyspace", config.getKeyspaceOrThrow())
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
     *    Always keep {@link CassandraKeyValueServiceConfig#poolSize()} connections around, per host. Allow bursting
     *    up to {@link CassandraKeyValueServiceConfig#maxConnectionBurstSize()} connections per host under load.
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
     * In an asynchronous thread (using default values):
     *    Every 20-30 seconds, examine approximately a tenth of the connections in pool.
     *    Discard any connections in this tenth of the pool whose TCP connections are closed.
     *    Discard any connections in this tenth of the pool that have been idle for more than 10 minutes,
     *       while still keeping a minimum number of idle connections around for fast borrows.
     */
    private GenericObjectPool<CassandraClient> createClientPool() {
        CassandraClientFactory cassandraClientFactory =
                new CassandraClientFactory(metricsManager, qosClient, host, config);
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

        poolConfig.setMinIdle(config.poolSize());
        poolConfig.setMaxIdle(config.maxConnectionBurstSize());
        poolConfig.setMaxTotal(config.maxConnectionBurstSize());

        // immediately throw when we try and borrow from a full pool; dealt with at higher level
        poolConfig.setBlockWhenExhausted(false);
        poolConfig.setMaxWaitMillis(config.socketTimeoutMillis());

        // this test is free/just checks a boolean and does not block; borrow is still fast
        poolConfig.setTestOnBorrow(true);

        poolConfig.setMinEvictableIdleTimeMillis(
                TimeUnit.MILLISECONDS.convert(config.idleConnectionTimeoutSeconds(), TimeUnit.SECONDS));
        // the randomness here is to prevent all of the pools for all of the hosts
        // evicting all at at once, which isn't great for C*.
        int timeBetweenEvictionsSeconds = config.timeBetweenConnectionEvictionRunsSeconds();
        int delta = ThreadLocalRandom.current().nextInt(Math.min(timeBetweenEvictionsSeconds / 2, 10));
        poolConfig.setTimeBetweenEvictionRunsMillis(
                TimeUnit.MILLISECONDS.convert(timeBetweenEvictionsSeconds + delta, TimeUnit.SECONDS));
        poolConfig.setNumTestsPerEvictionRun(-(int) (1.0 / config.proportionConnectionsToCheckPerEvictionRun()));
        poolConfig.setTestWhileIdle(true);

        poolConfig.setJmxNamePrefix(CassandraLogHelper.host(host));
        GenericObjectPool<CassandraClient> pool = new GenericObjectPool<>(cassandraClientFactory, poolConfig);
        registerMetrics(pool);
        return pool;
    }

    private void registerMetrics(GenericObjectPool<CassandraClient> pool) {
        registerPoolMetric("meanActiveTimeMillis", pool::getMeanActiveTimeMillis);
        registerPoolMetric("meanIdleTimeMillis", pool::getMeanIdleTimeMillis);
        registerPoolMetric("meanBorrowWaitTimeMillis", pool::getMeanBorrowWaitTimeMillis);
        registerPoolMetric("numIdle", pool::getNumIdle);
        registerPoolMetric("numActive", pool::getNumActive);
        registerPoolMetric("approximatePoolSize", () -> pool.getNumIdle() + pool.getNumActive());
        registerPoolMetric("proportionDestroyedByEvictor",
                () -> ((double) pool.getDestroyedByEvictorCount()) / ((double) pool.getCreatedCount()));
        registerPoolMetric("proportionDestroyedByBorrower",
                () -> ((double) pool.getDestroyedByBorrowValidationCount()) / ((double) pool.getCreatedCount()));
    }

    private void registerPoolMetric(String metricName, Gauge gauge) {
        metricsManager.registerOrAddToMetric(
                CassandraClientPoolingContainer.class,
                metricName,
                gauge,
                ImmutableMap.of("pool", "pool" + poolNumber));
    }
}
