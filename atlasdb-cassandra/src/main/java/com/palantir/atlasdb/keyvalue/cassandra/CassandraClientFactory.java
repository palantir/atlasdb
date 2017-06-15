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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.cassandra.CassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.remoting2.config.ssl.SslSocketFactories;

public class CassandraClientFactory extends BasePooledObjectFactory<Client> {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientFactory.class);

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

    private final InetSocketAddress addr;
    private final CassandraKeyValueServiceConfig config;

    public CassandraClientFactory(InetSocketAddress addr, CassandraKeyValueServiceConfig config) {
        this.addr = addr;
        this.config = config;
    }

    @Override
    public Client create() throws Exception {
        try {
            return getClient(addr, config);
        } catch (Exception e) {
            String message = String.format("Failed to construct client for %s/%s", addr, config.keyspace());
            if (config.usingSsl()) {
                message += " over SSL";
            }
            throw new ClientCreationFailedException(message, e);
        }
    }

    private static Cassandra.Client getClient(InetSocketAddress addr,
                                              CassandraKeyValueServiceConfig config) throws Exception {
        Client ret = getClientInternal(addr, config);
        try {
            ret.set_keyspace(config.keyspace());
            log.debug("Created new client for {}/{}{}{}",
                    addr,
                    config.keyspace(),
                    config.usingSsl() ? " over SSL" : "",
                    config.credentials().isPresent() ? " as user " + config.credentials().get().username() : "");
            return ret;
        } catch (Exception e) {
            ret.getOutputProtocol().getTransport().close();
            throw e;
        }
    }

    static Cassandra.Client getClientInternal(InetSocketAddress addr, CassandraKeyValueServiceConfig config)
            throws TException {
        TSocket thriftSocket = new TSocket(addr.getHostString(), addr.getPort(), config.socketTimeoutMillis());
        thriftSocket.open();
        try {
            thriftSocket.getSocket().setKeepAlive(true);
            thriftSocket.getSocket().setSoTimeout(config.socketQueryTimeoutMillis());
        } catch (SocketException e) {
            log.error("Couldn't set socket keep alive for {}", addr);
        }

        if (config.usingSsl()) {
            boolean success = false;
            try {
                final SSLSocketFactory factory;
                if (config.sslConfiguration().isPresent()) {
                    factory = SslSocketFactories.createSslSocketFactory(config.sslConfiguration().get());
                } else {
                    factory = sslSocketFactories.getUnchecked(addr);
                }
                SSLSocket socket = (SSLSocket) factory.createSocket(
                        thriftSocket.getSocket(),
                        addr.getHostString(),
                        addr.getPort(),
                        true);
                thriftSocket = new TSocket(socket);
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

        if (config.credentials().isPresent()) {
            try {
                login(client, config.credentials().get());
            } catch (TException e) {
                client.getOutputProtocol().getTransport().close();
                log.error("Exception thrown attempting to authenticate with config provided credentials", e);
                throw e;
            }
        }

        return client;
    }

    private static void login(Client client, CassandraCredentialsConfig config) throws TException {
        Map<String, String> credsMap = Maps.newHashMap();
        credsMap.put("username", config.username());
        credsMap.put("password", config.password());
        client.login(new AuthenticationRequest(credsMap));
    }

    @Override
    public boolean validateObject(PooledObject<Client> client) {
        return client.getObject().getOutputProtocol().getTransport().isOpen();
    }

    @Override
    public PooledObject<Client> wrap(Client client) {
        return new DefaultPooledObject<Client>(client);
    }

    @Override
    public void destroyObject(PooledObject<Client> client) {
        client.getObject().getOutputProtocol().getTransport().close();
        log.debug("Closed transport for client {}", client.getObject());
    }

    static class ClientCreationFailedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        ClientCreationFailedException(String message, Exception cause) {
            super(message, cause);
        }

        @Override
        public Exception getCause() {
            return (Exception) super.getCause();
        }
    }
}
