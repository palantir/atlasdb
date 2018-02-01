/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public abstract class AbstractRequestExceptionHandler {
    static final Logger log = LoggerFactory.getLogger(CassandraClientPool.class);

    final Supplier<Integer> maxTriesSameHost;
    final Supplier<Integer> maxTriesTotal;
    final Blacklist blacklist;

    AbstractRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost,
            Supplier<Integer> maxTriesTotal,
            Blacklist blacklist) {
        this.maxTriesSameHost = maxTriesSameHost;
        this.maxTriesTotal = maxTriesTotal;
        this.blacklist = blacklist;
    }

    @SuppressWarnings("unchecked")
    <K extends Exception> void handleExceptionFromRequest(
            RetryableCassandraRequest<?, K> req,
            InetSocketAddress hostTried,
            Exception ex)
            throws K {
        if (!isRetryable(ex)) {
            throw (K) ex;
        }

        req.triedOnHost(hostTried);
        int numberOfAttempts = req.getNumberOfAttempts();

        if (numberOfAttempts >= maxTriesTotal.get()) {
            logAndThrowException(numberOfAttempts, ex);
        }

        if (shouldBlacklist(ex, numberOfAttempts)) {
            blacklist.add(hostTried);
        }

        logNumberOfAttempts(ex, numberOfAttempts);
        handleBackoff(req, hostTried, ex);
        handleRetryOnDifferentHosts(req, hostTried, ex);
    }

    @SuppressWarnings("unchecked")
    <K extends Exception> void logAndThrowException(int numberOfAttempts, Exception ex) throws K {
        if (ex instanceof TTransportException
                && ex.getCause() != null
                && (ex.getCause().getClass() == SocketException.class)) {
            log.error(CONNECTION_FAILURE_MSG, numberOfAttempts, ex);
            String errorMsg =
                    MessageFormatter.format(CONNECTION_FAILURE_MSG, numberOfAttempts).getMessage();
            throw (K) new TTransportException(((TTransportException) ex).getType(), errorMsg, ex);
        } else {
            log.error("Tried to connect to cassandra {} times.",
                    SafeArg.of("numTries", numberOfAttempts), ex);
            throw (K) ex;
        }
    }

    @SuppressWarnings("unchecked")
    <K extends Exception> void logNumberOfAttempts(Exception ex, int numberOfAttempts) throws K {
        // Only log the actual exception the first time
        if (numberOfAttempts > 1) {
            log.info("Error occurred talking to cassandra. Attempt {} of {}. Exception message was: {} : {}",
                    SafeArg.of("numTries", numberOfAttempts),
                    SafeArg.of("maxTotalTries", maxTriesTotal),
                    SafeArg.of("exceptionClass", ex.getClass().getTypeName()),
                    UnsafeArg.of("exceptionMessage", ex.getMessage()));
        } else {
            log.info("Error occurred talking to cassandra. Attempt {} of {}.",
                    SafeArg.of("numTries", numberOfAttempts),
                    SafeArg.of("maxTotalTries", maxTriesTotal),
                    ex);
        }
    }

    abstract boolean shouldBlacklist(Exception ex, int numberOfAttempts);

    abstract <K extends Exception> void handleBackoff(RetryableCassandraRequest<?, K> req,
            InetSocketAddress hostTried,
            Exception ex);

    abstract <K extends Exception> void handleRetryOnDifferentHosts(RetryableCassandraRequest<?, K> req,
            InetSocketAddress hostTried, Exception ex);

    // Determine the behavior we want from each type of exception.

    @VisibleForTesting
    boolean isRetryable(Exception ex) {
        return isConnectionException(ex)
                || isTransientException(ex)
                || isIndicativeOfCassandraLoad(ex)
                || isFastFailoverException(ex);
    }

    // Group exceptions by type.

    static boolean isConnectionException(Throwable ex) {
        return ex != null
                // tcp socket timeout, possibly indicating network flake, long GC, or restarting server.
                && (ex instanceof SocketTimeoutException
                || ex instanceof CassandraClientFactory.ClientCreationFailedException
                || isConnectionException(ex.getCause()));
    }

    private static boolean isTransientException(Throwable ex) {
        return ex != null
                // There's a problem with the connection to Cassandra.
                && (ex instanceof TTransportException
                // Cassandra timeout. Maybe took too long to CAS, or Cassandra is under load.
                || ex instanceof TimedOutException
                // Not enough Cassandra nodes are up.
                || ex instanceof InsufficientConsistencyException
                || isTransientException(ex.getCause()));
    }

    boolean isIndicativeOfCassandraLoad(Throwable ex) {
        return ex != null
                // pool for this node is fully in use
                && (ex instanceof NoSuchElementException
                // remote cassandra node couldn't talk to enough other remote cassandra nodes to answer
                || ex instanceof UnavailableException
                || isIndicativeOfCassandraLoad(ex.getCause()));
    }

    boolean isFastFailoverException(Throwable ex) {
        return ex != null
                // underlying cassandra table does not exist. The table might exist on other cassandra nodes.
                && (ex instanceof InvalidRequestException
                || isFastFailoverException(ex.getCause()));
    }

    private static final String CONNECTION_FAILURE_MSG = "Tried to connect to cassandra {} times."
            + " Error writing to Cassandra socket."
            + " Likely cause: Exceeded maximum thrift frame size;"
            + " unlikely cause: network issues.";
}
