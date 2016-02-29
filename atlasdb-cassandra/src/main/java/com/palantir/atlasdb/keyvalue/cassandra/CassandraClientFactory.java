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
import java.net.InetSocketAddress;
import java.net.SocketException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.palantir.remoting.ssl.SslConfiguration;
import com.palantir.remoting.ssl.SslSocketFactories;

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
    private final String keyspace;
    private final boolean isSsl;
    private final Optional<SslConfiguration> sslConfiguration;
    private final int socketTimeoutMillis;
    private final int socketQueryTimeoutMillis;

    public CassandraClientFactory(InetSocketAddress addr,
                                  String keyspace,
                                  boolean isSsl,
                                  Optional<SslConfiguration> sslConfiguration,
                                  int socketTimeoutMillis,
                                  int socketQueryTimeoutMillis) {
        this.addr = addr;
        this.keyspace = keyspace;
        this.isSsl = isSsl;
        this.sslConfiguration = sslConfiguration;
        this.socketTimeoutMillis = socketTimeoutMillis;
        this.socketQueryTimeoutMillis = socketQueryTimeoutMillis;
    }

    @Override
    public Client create() throws Exception {
        try {
            return getClient(addr, keyspace, isSsl, sslConfiguration, socketTimeoutMillis, socketQueryTimeoutMillis);
        } catch (Exception e) {
            String message = String.format("Failed to construct client for %s/%s", addr, keyspace);
            if (isSsl) {
                message += " over SSL";
            }
            throw new ClientCreationFailedException(message, e);
        }
    }

    private static Cassandra.Client getClient(InetSocketAddress addr,
                                              String keyspace,
                                              boolean isSsl,
                                              Optional<SslConfiguration> sslConfiguration,
                                              int socketTimeoutMillis,
                                              int socketQueryTimeoutMillis) throws Exception {
        Client ret = getClientInternal(addr, isSsl, sslConfiguration, socketTimeoutMillis, socketQueryTimeoutMillis);
        try {
            ret.set_keyspace(keyspace);
            log.info("Created new client for {}/{} {}", addr, keyspace, (isSsl ? "over SSL" : ""));
            return ret;
        } catch (Exception e) {
            ret.getOutputProtocol().getTransport().close();
            throw e;
        }
    }

    public static Cassandra.Client getClientInternal(InetSocketAddress addr,
                                                     boolean isSsl,
                                                     Optional<SslConfiguration> sslConfiguration,
                                                     int socketTimeoutMillis,
                                                     int socketQueryTimeoutMillis) throws TTransportException {
        TSocket tSocket = new TSocket(addr.getHostString(), addr.getPort(), socketTimeoutMillis);
        tSocket.open();
        try {
            tSocket.getSocket().setKeepAlive(true);
            tSocket.getSocket().setSoTimeout(socketQueryTimeoutMillis);
        } catch (SocketException e) {
            log.error("Couldn't set socket keep alive for {}", addr);
        }

        if (isSsl) {
            boolean success = false;
            try {
                final SSLSocketFactory factory;
                if (sslConfiguration.isPresent()) {
                    factory = SslSocketFactories.createSslSocketFactory(sslConfiguration.get());
                } else {
                    factory = sslSocketFactories.getUnchecked(addr);
                }
                SSLSocket socket = (SSLSocket) factory.createSocket(tSocket.getSocket(), addr.getHostString(), addr.getPort(), true);
                tSocket = new TSocket(socket);
                success = true;
            } catch (IOException e) {
                throw new TTransportException(e);
            } finally {
                if (!success) {
                    tSocket.close();
                }
            }
        }
        TTransport tFramedTransport =
                new TFramedTransport(tSocket, CassandraConstants.CLIENT_MAX_THRIFT_FRAME_SIZE_BYTES);
        TProtocol protocol = new TBinaryProtocol(tFramedTransport);
        Cassandra.Client client = new Cassandra.Client(protocol);
        return client;
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
        log.info("Closed transport for client {}", client.getObject());
    }

    static class ClientCreationFailedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public ClientCreationFailedException(String message, Exception cause) {
            super(message, cause);
        }

        @Override
        public Exception getCause() {
            return (Exception) super.getCause();
        }
    }
}
