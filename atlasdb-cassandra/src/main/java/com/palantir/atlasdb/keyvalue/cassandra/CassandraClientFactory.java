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
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
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

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.cassandra.CassandraCredentialsConfig;

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
    private final Optional<CassandraCredentialsConfig> creds;
    private final boolean isSsl;
    private final int socketTimeoutMillis;
    private final int socketQueryTimeoutMillis;

    public CassandraClientFactory(InetSocketAddress addr,
                                  String keyspace,
                                  Optional<CassandraCredentialsConfig> creds,
                                  boolean isSsl,
                                  int socketTimeoutMillis,
                                  int socketQueryTimeoutMillis) {
        this.addr = addr;
        this.keyspace = keyspace;
        this.creds = creds;
        this.isSsl = isSsl;
        this.socketTimeoutMillis = socketTimeoutMillis;
        this.socketQueryTimeoutMillis = socketQueryTimeoutMillis;
    }

    @Override
    public Client create() throws Exception {
        try {
            return getClient(addr, keyspace, creds, isSsl, socketTimeoutMillis, socketQueryTimeoutMillis);
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
                                              Optional<CassandraCredentialsConfig> creds,
                                              boolean isSsl,
                                              int socketTimeoutMillis,
                                              int socketQueryTimeoutMillis) throws Exception {
        Client ret = getClientInternal(addr, creds, isSsl, socketTimeoutMillis, socketQueryTimeoutMillis);;
        try {
            ret.set_keyspace(keyspace);
            log.debug("Created new client for {}/{} {} {}", addr, keyspace, (isSsl ? "over SSL" : ""),
                    creds.isPresent() ? " as user " + creds.get().username() : "");
            return ret;
        } catch (Exception e) {
            ret.getOutputProtocol().getTransport().close();
            throw e;
        }
    }

    static Cassandra.Client getClientInternal(InetSocketAddress addr,
                                                     Optional<CassandraCredentialsConfig> creds,
                                                     boolean isSsl,
                                                     int socketTimeoutMillis,
                                                     int socketQueryTimeoutMillis) throws TException {
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
                SSLSocketFactory factory = sslSocketFactories.getUnchecked(addr);
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

        if (creds.isPresent()) {
            try {
                login(client, creds.get());
            } catch (TException e) {
                client.getOutputProtocol().getTransport().close();
                log.error("Exception thrown attempting to authenticate with config provided credentials", e);
                throw e;
            }
        }

        return client;
    }

    private static void login(Client client, CassandraCredentialsConfig config) throws AuthenticationException,
                                                                                       AuthorizationException,
                                                                                       TException {
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

        public ClientCreationFailedException(String message, Exception cause) {
            super(message, cause);
        }

        @Override
        public Exception getCause() {
            return (Exception) super.getCause();
        }
    }
}
