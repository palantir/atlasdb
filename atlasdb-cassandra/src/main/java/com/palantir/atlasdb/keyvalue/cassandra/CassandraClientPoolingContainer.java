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
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.pooling.AbstractPoolingContainer;

/**
 * This class will run the passed function with a valid client with an open socket.  An open socket
 * is not a guarantee that it will actually move bytes over the wire.  The default tcp keep alive is
 * normally set to 2 hours, so it may be the case that the other side of the socket has been dead for
 * almost 2 hours.
 * <p>
 * This class will not reuse a socket that has experienced a TTransportException because that socket
 * may not read the TProtocol correctly anymore.
 * <p>
 * This class will return an instance of ClientCreationFailedException if the socket could not be
 * opened successfully or the current keyspace could not be set.
 */
public class CassandraClientPoolingContainer extends AbstractPoolingContainer<Client> {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolingContainer.class);
    private static final LoadingCache<String, SSLSocketFactory> sslSocketFactories =
            CacheBuilder.newBuilder().build(new CacheLoader<String, SSLSocketFactory>() {
                @Override
                public SSLSocketFactory load(String host) throws Exception {
                    /*
                     * Use a separate SSLSocketFactory per host to reduce contention on the synchronized method
                     * SecureRandom.nextBytes. Otherwise, this is identical to SSLSocketFactory.getDefault()
                     */
                    return SSLContext.getInstance("Default").getSocketFactory();
                }
            });

    private final String host;
    private final String keyspace;
    private final int port;
    private final boolean isSsl;
    private final int socketTimeoutMillis;
    private final int socketQueryTimeoutMillis;
    private final AtomicLong count = new AtomicLong();

    private CassandraClientPoolingContainer(Builder builder){
        super(builder.poolSize);
        this.host = builder.host;
        this.port = builder.port;
        this.isSsl = builder.isSsl;
        this.keyspace = builder.keyspace;
        this.socketTimeoutMillis = builder.socketTimeoutMillis;
        this.socketQueryTimeoutMillis = builder.socketQueryTimeoutMillis;
    }

    @Override
    @Nonnull
    protected Client createNewPooledResource() {
        try {
            return getClient(host, port, keyspace, isSsl, socketTimeoutMillis, socketQueryTimeoutMillis);
        } catch (Exception e) {
            throw new ClientCreationFailedException("Failed to construct client for host: " + host, e);
        }
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
        Client resource = getGoodClient();

        try {
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
                discardCurrentPool();
            }
            throw (K) e;
        } finally {
            if (shouldReuse) {
                log.info("Returning {} to pool", resource);
                returnResource(resource);
            } else {
                log.info("Discarding: {}", resource);
                cleanupForDiscard(resource);
            }
        }
    }

    private Client getGoodClient() {
        Client resource = null;
        do {
            if (resource != null) {
                cleanupForDiscard(resource);
            }
            resource = getResource();
        } while (!resource.getOutputProtocol().getTransport().isOpen());
        return resource;
    }

    @Override
    protected void cleanupForDiscard(Client discardedResource) {
        discardedResource.getOutputProtocol().getTransport().close();
        log.info("Closed transport for client {}", discardedResource);
        super.cleanupForDiscard(discardedResource);
    }

    public static class Builder {
        private final String host;
        private final int port;

        // default values are provided
        private int poolSize = 20;
        private String keyspace = "atlasdb";
        private boolean isSsl = false;
        private int socketTimeoutMillis = 2000;
        private int socketQueryTimeoutMillis = 62000;

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

        public CassandraClientPoolingContainer build(){
            return new CassandraClientPoolingContainer(this);
        }
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

    private static Cassandra.Client getClient(String host, int port, String keyspace, boolean isSsl, int socketTimeoutMillis, int socketQueryTimeoutMillis) throws Exception {
        Client ret = getClientInternal(host, port, isSsl, socketTimeoutMillis, socketQueryTimeoutMillis);
        try {
            ret.set_keyspace(keyspace);
            log.info("Created new client for {}:{}/{} {}", host, port, keyspace, (isSsl ? "over SSL" : ""));
            return ret;
        } catch (Exception e) {
            ret.getOutputProtocol().getTransport().close();
            throw e;
        }
    }

    public static Cassandra.Client getClientInternal(String host, int port, boolean isSsl, int socketTimeoutMillis, int socketQueryTimeoutMillis) throws TTransportException {
        TSocket tSocket = new TSocket(host, port, socketTimeoutMillis);
        tSocket.open();
        try {
            tSocket.getSocket().setKeepAlive(true);
            tSocket.getSocket().setSoTimeout(socketQueryTimeoutMillis);
       } catch (SocketException e1) {
            log.error("Couldn't set socket keep alive for " + host);
        }

        if (isSsl) {
            boolean success = false;
            try {
                SSLSocketFactory factory = sslSocketFactories.getUnchecked(host);
                SSLSocket socket = (SSLSocket) factory.createSocket(tSocket.getSocket(), host, port, true);
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
        TTransport tFramedTransport = new TFramedTransport(tSocket, CassandraConstants.CLIENT_MAX_THRIFT_FRAME_SIZE_BYTES);
        TProtocol protocol = new TBinaryProtocol(tFramedTransport);
        Cassandra.Client client = new Cassandra.Client(protocol);
        return client;
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
