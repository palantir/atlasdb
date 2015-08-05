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

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Random;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer.ClientCreationFailedException;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.pooling.ForwardingPoolingContainer;
import com.palantir.common.pooling.PoolingContainer;

public class RetriablePoolingContainer extends ForwardingPoolingContainer<Client> {
    private static final Logger log = LoggerFactory.getLogger(RetriablePoolingContainer.class);
    private static final int MAX_TRIES = 10;

    private final PoolingContainer<Client> delegate;

    public RetriablePoolingContainer(PoolingContainer<Client> delegate) {
        this.delegate = delegate;
    }

    @Override
    protected PoolingContainer<Client> delegate() {
        return delegate;
    }

    @Override
    public <V, K extends Exception> V runWithPooledResource(FunctionCheckedException<Client, V, K> f) throws K {
        int numTries = 0;
        while (true) {
            try {
                return super.runWithPooledResource(f);
            } catch (Exception e) {
                numTries++;
                this.<K>handleException(numTries, e);
            }
        }
    }

    @Override
    public <V> V runWithPooledResource(Function<Client, V> f) {
        throw new UnsupportedOperationException(
                "you should use FunctionCheckedException<?, ?, Exception> " +
                "to ensure the TTransportException type is propagated correctly.");
    }

    @SuppressWarnings("unchecked")
    private <K extends Exception> void handleException(int numTries, Exception e) throws K {
        if (e instanceof ClientCreationFailedException
                || e instanceof TTransportException
                || e instanceof TimedOutException
                || e instanceof SocketTimeoutException
                || e instanceof UnavailableException) {
            if (numTries >= MAX_TRIES) {
                if (e instanceof TTransportException
                        && e.getCause() != null
                        && (e.getCause().getClass() == SocketException.class)) {
                    String msg = "Error writing to Cassandra socket. Likely cause: Exceeded maximum thrift frame size; unlikely cause: network issues.";
                    log.error("Tried to connect to cassandra " + numTries + " times. " + msg, e);
                    e = new TTransportException(((TTransportException) e).getType(), msg, e);
                } else {
                    log.error("Tried to connect to cassandra " + numTries + " times.", e);
                }
                throw (K) e;
            } else {
                log.warn("Transport failure to cassandra. We will retry.", e);
                if (e instanceof SocketTimeoutException
                        || e instanceof UnavailableException) {
                    // Connection is no good? This may be due to a long GC, we should back off sending them requests.
                    // Binary exponential backoff; should in total take ~10s on average w/MAX_TRIES=10
                    try {
                        Thread.sleep(new Random().nextInt((1 << numTries)-1)*20);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } else {
            throw (K) e;
        }
    }

}
