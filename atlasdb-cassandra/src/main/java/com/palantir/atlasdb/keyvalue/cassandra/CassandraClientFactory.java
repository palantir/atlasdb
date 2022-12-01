/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.cassandra.ImmutableCassandraClientConfig.SocketTimeoutMillisBuildStage;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.annotations.ImmutablesStyles.StagedBuilderStyle;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.exception.SafeSSLPeerUnverifiedException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.TimedRunner;
import com.palantir.util.TimedRunner.TaskContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.immutables.value.Value;

public class CassandraClientFactory extends BasePooledObjectFactory<CassandraClient> {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraClientFactory.class);
    private static final LoadingCache<SslConfiguration, SSLSocketFactory> sslSocketFactoryCache =
            Caffeine.newBuilder().weakValues().build(SslSocketFactories::createSslSocketFactory);

    private static final DefaultHostnameVerifier hostnameVerifier = new DefaultHostnameVerifier();

    private final MetricsManager metricsManager;
    private final CassandraServer cassandraServer;
    private final CassandraClientConfig clientConfig;
    private final SSLSocketFactory sslSocketFactory;
    private final TimedRunner timedRunner;
    private final TSocketFactory tSocketFactory;

    public CassandraClientFactory(
            MetricsManager metricsManager, CassandraServer cassandraServer, CassandraClientConfig clientConfig) {
        this.metricsManager = metricsManager;
        this.cassandraServer = cassandraServer;
        this.clientConfig = clientConfig;
        this.sslSocketFactory = createSslSocketFactory(clientConfig.sslConfiguration());
        this.timedRunner = TimedRunner.create(clientConfig.timeoutOnConnectionClose());
        this.tSocketFactory = new InstrumentedTSocket.Factory(metricsManager);
    }

    @Override
    public CassandraClient create() {
        try {
            return instrumentClient(getRawClientWithKeyspaceSet());
        } catch (Exception e) {
            String message = String.format(
                    "Failed to construct client for %s/%s", cassandraServer.proxy(), clientConfig.keyspace());
            if (clientConfig.usingSsl()) {
                message += " over SSL";
            }
            throw new ClientCreationFailedException(message, e);
        }
    }

    private CassandraClient instrumentClient(Client rawClient) {
        CassandraClient client = new CassandraClientImpl(rawClient);
        // TODO(ssouza): use the kvsMethodName to tag the timers.
        client = AtlasDbMetrics.instrumentTimed(metricsManager.getRegistry(), CassandraClient.class, client);
        client = new ProfilingCassandraClient(client);
        client = new TracingCassandraClient(client);
        client = new InstrumentedCassandraClient(client, metricsManager.getTaggedRegistry());
        client = QosCassandraClient.instrumentWithMetrics(client, metricsManager);
        return client;
    }

    private Cassandra.Client getRawClientWithKeyspaceSet() throws TException {
        Client ret = getRawClientWithTimedCreation();
        try {
            ret.set_keyspace(clientConfig.keyspace());
            if (log.isDebugEnabled()) {
                log.debug(
                        "Created new client for {}/{}{}{}",
                        SafeArg.of("address", CassandraLogHelper.host(cassandraServer.proxy())),
                        UnsafeArg.of("keyspace", clientConfig.keyspace()),
                        SafeArg.of("usingSsl", clientConfig.usingSsl() ? " over SSL" : ""),
                        UnsafeArg.of(
                                "usernameConfig",
                                " as user " + clientConfig.credentials().username()));
            }
            return ret;
        } catch (TException e) {
            ret.getOutputProtocol().getTransport().close();
            throw e;
        }
    }

    static CassandraClient getClientInternal(CassandraServer cassandraServer, CassandraClientConfig clientConfig)
            throws TException {
        return new CassandraClientImpl(getRawClient(
                cassandraServer,
                clientConfig,
                createSslSocketFactory(clientConfig.sslConfiguration()),
                TSocketFactory.Default.INSTANCE));
    }

    private static SSLSocketFactory createSslSocketFactory(Optional<SslConfiguration> sslConfiguration) {
        return sslConfiguration
                .map(sslSocketFactoryCache::get)
                // This path should never be hit in production code, since we expect SSL is configured explicitly.
                .orElseGet(() -> (SSLSocketFactory) SSLSocketFactory.getDefault());
    }

    private Cassandra.Client getRawClientWithTimedCreation() throws TException {
        Timer clientCreation = metricsManager.registerOrGetTimer(CassandraClientFactory.class, "clientCreation");
        try (Timer.Context timer = clientCreation.time()) {
            return getRawClient(cassandraServer, clientConfig, sslSocketFactory, tSocketFactory);
        }
    }

    private static Cassandra.Client getRawClient(
            CassandraServer cassandraServer,
            CassandraClientConfig clientConfig,
            SSLSocketFactory sslSocketFactory,
            TSocketFactory tSocketFactory)
            throws TException {
        InetSocketAddress addr = cassandraServer.proxy();
        TSocket thriftSocket =
                tSocketFactory.create(addr.getHostString(), addr.getPort(), clientConfig.socketTimeoutMillis());
        thriftSocket.open();
        setSocketOptions(
                thriftSocket,
                socket -> {
                    socket.getSocket().setKeepAlive(true);
                    socket.getSocket().setSoTimeout(clientConfig.initialSocketQueryTimeoutMillis());
                },
                addr);

        if (clientConfig.usingSsl()) {
            boolean success = false;
            try {
                SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket(
                        thriftSocket.getSocket(), addr.getHostString(), addr.getPort(), true);
                thriftSocket = tSocketFactory.create(socket);
                verifyEndpoint(cassandraServer, socket, clientConfig.enableEndpointVerification());
                success = true;
            } catch (IOException e) {
                throw new TTransportException(e);
            } finally {
                if (!success) {
                    thriftSocket.close();
                }
            }
        }
        TTransport thriftFramedTransport =
                new TFramedTransport(thriftSocket, CassandraConstants.CLIENT_MAX_THRIFT_FRAME_SIZE_BYTES);
        TProtocol protocol = new TBinaryProtocol(thriftFramedTransport);
        Cassandra.Client client = new Cassandra.Client(protocol);

        try {
            login(client, clientConfig.credentials());
            setSocketOptions(
                    thriftSocket,
                    socket -> socket.getSocket().setSoTimeout(clientConfig.socketQueryTimeoutMillis()),
                    addr);
        } catch (TException e) {
            client.getOutputProtocol().getTransport().close();
            log.error("Exception thrown attempting to authenticate with config provided credentials", e);
            throw e;
        }

        return client;
    }

    private static void login(Client client, CassandraCredentialsConfig config) throws TException {
        Map<String, String> credsMap = new HashMap<>();
        credsMap.put("username", config.username());
        credsMap.put("password", config.password());
        client.login(new AuthenticationRequest(credsMap));
    }

    /**
     * Verifies that the current SSL connection hostname/ip address matches what the certificate has served.
     * This will check both ip address/hostname, and uses the IP address associated with the socket, rather
     * that what has been provided. Hostname/ip address are both need to be checked, as historically we've
     * connected to Cassandra directly using IP addresses, and therefore need to support such cases.
     *
     * Will only throw when throwOnFailure is true, even if the socket is closed during verification.
     */
    @VisibleForTesting
    static void verifyEndpoint(CassandraServer cassandraServer, SSLSocket socket, boolean throwOnFailure)
            throws SafeSSLPeerUnverifiedException {
        Set<String> endpointsToCheck = getEndpointsToCheck(cassandraServer, socket);
        boolean endpointVerified =
                endpointsToCheck.stream().anyMatch(address -> hostnameVerifier.verify(address, socket.getSession()));
        if (socket.isClosed()) {
            if (throwOnFailure) {
                throw new SafeSSLPeerUnverifiedException(
                        "Unable to verify endpoints as socket is closed.",
                        SafeArg.of("endpoint", socket.getInetAddress()));
            }
            return;
        }
        if (!endpointVerified) {
            log.warn("Endpoint verification failed for host.", SafeArg.of("endpointsChecked", endpointsToCheck));
            if (throwOnFailure) {
                throw new SafeSSLPeerUnverifiedException(
                        "Endpoint verification failed for host.", SafeArg.of("endpointsChecked", endpointsToCheck));
            }
        }
    }

    @VisibleForTesting
    static Set<String> getEndpointsToCheck(CassandraServer cassandraServer, SSLSocket socket) {
        return ImmutableSet.of(socket.getInetAddress().getHostAddress(), cassandraServer.cassandraHostName());
    }

    @Override
    public boolean validateObject(PooledObject<CassandraClient> client) {
        try {
            return client.getObject().getOutputProtocol().getTransport().isOpen();
        } catch (Throwable t) {
            log.info(
                    "Failed when attempting to validate a Cassandra client in the Cassandra client pool."
                            + " Defensively believing that this object is NOT valid.",
                    SafeArg.of("cassandraClient", CassandraLogHelper.host(cassandraServer.proxy())),
                    t);
            return false;
        }
    }

    @Override
    public PooledObject<CassandraClient> wrap(CassandraClient client) {
        return new DefaultPooledObject<>(client);
    }

    @Override
    public void destroyObject(PooledObject<CassandraClient> client) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Attempting to close transport for client {} of host {}",
                    UnsafeArg.of("client", client),
                    SafeArg.of("cassandraClient", CassandraLogHelper.host(cassandraServer.proxy())));
        }
        try {
            TaskContext<Void> taskContext =
                    TaskContext.createRunnable(() -> client.getObject().close(), () -> {});
            timedRunner.run(taskContext);
        } catch (Throwable t) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Failed to close transport for client {} of host {}",
                        UnsafeArg.of("client", client),
                        SafeArg.of("cassandraClient", CassandraLogHelper.host(cassandraServer.proxy())),
                        t);
            }
            throw new SafeRuntimeException("Threw while attempting to close transport for client", t);
        }
        if (log.isDebugEnabled()) {
            log.debug(
                    "Closed transport for client {} of host {}",
                    UnsafeArg.of("client", client),
                    SafeArg.of("cassandraClient", CassandraLogHelper.host(cassandraServer.proxy())));
        }
    }

    static class ClientCreationFailedException extends AtlasDbDependencyException {
        private static final long serialVersionUID = 1L;

        ClientCreationFailedException(String message, Exception cause) {
            super(message, cause);
        }

        @Override
        public Exception getCause() {
            return (Exception) super.getCause();
        }
    }

    private static void setSocketOptions(TSocket thriftSocket, SocketConsumer consumer, InetSocketAddress addr) {
        try {
            consumer.accept(thriftSocket);
        } catch (SocketException e) {
            log.error(
                    "Couldn't set socket options for host {}", SafeArg.of("address", CassandraLogHelper.host(addr)), e);
        }
    }

    @FunctionalInterface
    private interface SocketConsumer {
        void accept(TSocket value) throws SocketException;
    }

    @Value.Immutable
    @StagedBuilderStyle
    interface CassandraClientConfig {
        int socketTimeoutMillis();

        int socketQueryTimeoutMillis();

        int initialSocketQueryTimeoutMillis();

        CassandraCredentialsConfig credentials();

        boolean usingSsl();

        boolean enableEndpointVerification();

        Optional<SslConfiguration> sslConfiguration();

        String keyspace();

        Duration timeoutOnConnectionClose();

        static CassandraClientConfig of(CassandraKeyValueServiceConfig config) {
            return builder()
                    .socketTimeoutMillis(config.socketTimeoutMillis())
                    .socketQueryTimeoutMillis(config.socketQueryTimeoutMillis())
                    .initialSocketQueryTimeoutMillis(config.initialSocketQueryTimeoutMillis())
                    .credentials(config.credentials())
                    .usingSsl(config.usingSsl())
                    .enableEndpointVerification(config.enableEndpointVerification())
                    .keyspace(config.getKeyspaceOrThrow())
                    .timeoutOnConnectionClose(config.timeoutOnConnectionClose())
                    .sslConfiguration(config.sslConfiguration())
                    .build();
        }

        static SocketTimeoutMillisBuildStage builder() {
            return ImmutableCassandraClientConfig.builder();
        }
    }
}
