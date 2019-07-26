/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.cassandra.CassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.util.MetricsManager;

public final class AsyncCassandraClientFactory {

    private static final Logger log = LoggerFactory.getLogger(AsyncCassandraClientFactory.class);

    private static final LoadingCache<InetSocketAddress, SSLSocketFactory> sslSocketFactories =
            CacheBuilder.newBuilder().build(new CacheLoader<InetSocketAddress, SSLSocketFactory>() {
                @Override
                public SSLSocketFactory load(InetSocketAddress host) throws Exception {
                    /*
                     * Use a separate SSLSocketFactory per host to reduce contention on the synchronized method
                     * SecureRandom.nextBytes. Otherwise, this is identical to SSLSocketFactory.getDefault()
                     */
                    return SSLContext.getInstance("Default").getSocketFactory();
                }
            });

    private final MetricsManager metricsManager;
    private final InetSocketAddress addr;
    private final CassandraKeyValueServiceConfig config;

    public AsyncCassandraClientFactory(MetricsManager metricsManager, InetSocketAddress addr,
            CassandraKeyValueServiceConfig config) {
        this.metricsManager = metricsManager;
        this.addr = addr;
        this.config = config;
    }

    public AsyncCassandraClient create(InetSocketAddress addr) throws Exception {
        //        try {
        //            return instrumentClient(getRawClientWithKeyspace(addr, config));
        //        } catch (Exception e) {
        //            String message = String.format("Failed to construct client for %s/%s", addr, config.getKeyspaceOrThrow());
        //            if (config.usingSsl()) {
        //                message += " over SSL";
        //            }
        //            throw new CassandraClientFactory.ClientCreationFailedException(message, e);
        //        }
        return null;
    }

    private static ListenableFuture<Cassandra.AsyncClient> getRawClient(InetSocketAddress addr,
            CassandraKeyValueServiceConfig config)
            throws TException {
        try {
            SocketChannel socketChannel = SocketChannel.open(
                    new InetSocketAddress(addr.getHostString(), addr.getPort()));
            socketChannel.configureBlocking(false);

            SSLEngine sslEngine = SSLContext.getDefault().createSSLEngine();
            sslEngine.setUseClientMode(true);

            // https://github.com/marianobarrios/tls-channel

            //            SSLSocketChannel sslSocketChannel = new SSLSocketChannel();

            //addr.getHostString(), addr.getPort(), config.socketTimeoutMillis()

            // TODO(jakubk): Would be nice if this was opening async
            TNonblockingTransport thriftSocket = null;
            try {
                thriftSocket = new TNonblockingSocket(socketChannel);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            thriftSocket.open();

            //        if (config.usingSsl()) {
            //            boolean success = false;
            //            try {
            //                final SSLSocketFactory factory;
            //                if (config.sslConfiguration().isPresent()) {
            //                    factory = SslSocketFactories.createSslSocketFactory(config.sslConfiguration().get());
            //                } else {
            //                    factory = sslSocketFactories.getUnchecked(addr);
            //                }
            //                SSLSocket socket = (SSLSocket) factory.createSocket(
            //                        thriftSocket.getSocket(),
            //                        addr.getHostString(),
            //                        addr.getPort(),
            //                        true);
            //                thriftSocket = new TSocket(socket);
            //                success = true;
            //            } catch (IOException e) {
            //                throw new TTransportException(e);
            //            } finally {
            //                if (!success) {
            //                    thriftSocket.close();
            //                }
            //            }
            //        }
            TTransport thriftFramedTransport =
                    new TFramedTransport(thriftSocket, CassandraConstants.CLIENT_MAX_THRIFT_FRAME_SIZE_BYTES);
            TProtocol protocol = new TBinaryProtocol(thriftFramedTransport);

            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

            // This is the selector thread, they should be shared really.
            TAsyncClientManager asyncClientManager;
            try {
                asyncClientManager = new TAsyncClientManager();
            } catch (IOException e) {
                throw new RuntimeException();
            }

            Cassandra.AsyncClient client = new Cassandra.AsyncClient(protocolFactory, asyncClientManager, thriftSocket);

            return Futures.transform(login(client, config.credentials()), $ -> client, MoreExecutors.directExecutor());
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static ListenableFuture<Void> login(Cassandra.AsyncClient client, CassandraCredentialsConfig config)
            throws TException {
        Map<String, String> credsMap = Maps.newHashMap();
        credsMap.put("username", config.username());
        credsMap.put("password", config.password());
        SettableFuture<Void> result = SettableFuture.create();
        client.login(new AuthenticationRequest(credsMap), new AsyncMethodCallback<Void>() {
            @Override
            public void onComplete(Void response) {
                result.set(null);
            }

            @Override
            public void onError(Exception exception) {
                result.setException(exception);
            }
        });
        return result;
    }
}
