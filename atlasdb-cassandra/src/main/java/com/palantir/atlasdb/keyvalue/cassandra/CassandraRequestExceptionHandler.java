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
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
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

class CassandraRequestExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(CassandraRequestExceptionHandler.class);

    private final Supplier<Integer> maxTriesSameHost;
    private final Supplier<Integer> maxTriesTotal;
    private final Supplier<Boolean> useConservativeHandler;
    private final Blacklist blacklist;

    CassandraRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost,
            Supplier<Integer> maxTriesTotal,
            Supplier<Boolean> useConservativeHandler,
            Blacklist blacklist) {
        this.maxTriesSameHost = maxTriesSameHost;
        this.maxTriesTotal = maxTriesTotal;
        this.useConservativeHandler = useConservativeHandler;
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

        RequestExceptionHandlerStrategy strategy = getStrategy();

        req.triedOnHost(hostTried);
        int numberOfAttempts = req.getNumberOfAttempts();

        if (numberOfAttempts >= maxTriesTotal.get()) {
            logAndThrowException(numberOfAttempts, ex);
        }

        if (shouldBlacklist(ex, numberOfAttempts)) {
            blacklist.add(hostTried);
        }

        logNumberOfAttempts(ex, numberOfAttempts);
        handleBackoff(req, hostTried, ex, strategy);
        handleRetryOnDifferentHosts(req, hostTried, ex, strategy);
    }

    @VisibleForTesting
    RequestExceptionHandlerStrategy getStrategy() {
        if (useConservativeHandler.get()) {
            return Conservative.INSTANCE;
        } else {
            return Default.INSTANCE;
        }
    }

    @SuppressWarnings("unchecked")
    private <K extends Exception> void logAndThrowException(int numberOfAttempts, Exception ex) throws K {
        if (ex instanceof TTransportException
                && ex.getCause() != null
                && (ex.getCause().getClass() == SocketException.class)) {
            log.warn(CONNECTION_FAILURE_MSG_WITH_EXCEPTION_INFO,
                    SafeArg.of("numTries", numberOfAttempts),
                    SafeArg.of("exceptionClass", ex.getClass().getTypeName()),
                    UnsafeArg.of("exceptionMessage", ex.getMessage()));
            String errorMsg = MessageFormatter.format(CONNECTION_FAILURE_MSG, numberOfAttempts).getMessage();
            throw (K) new TTransportException(((TTransportException) ex).getType(), errorMsg, ex);
        } else {
            log.warn("Tried to connect to cassandra {} times. Exception message was: {} : {}",
                    SafeArg.of("numTries", numberOfAttempts),
                    SafeArg.of("exceptionClass", ex.getClass().getTypeName()),
                    UnsafeArg.of("exceptionMessage", ex.getMessage()));
            throw (K) ex;
        }
    }

    @SuppressWarnings("unchecked")
    private <K extends Exception> void logNumberOfAttempts(Exception ex, int numberOfAttempts) throws K {
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

    // TODO(gmaretic): figure out if this needs to be changed
    @VisibleForTesting
    boolean shouldBlacklist(Exception ex, int numberOfAttempts) {
        return isConnectionException(ex) && numberOfAttempts >= maxTriesSameHost.get();
    }

    private <K extends Exception> void handleBackoff(RetryableCassandraRequest<?, K> req,
            InetSocketAddress hostTried,
            Exception ex, RequestExceptionHandlerStrategy strategy) {
        if (!shouldBackoff(ex, strategy)) {
            return;
        }

        log.info("Retrying a query, {}, with backoff of {}ms, intended for host {}.",
                UnsafeArg.of("queryString", req.getFunction().toString()),
                SafeArg.of("sleepDuration", strategy.getBackoffPeriod(req.getNumberOfAttempts())),
                SafeArg.of("hostName", CassandraLogHelper.host(hostTried)));

        try {
            Thread.sleep(strategy.getBackoffPeriod(req.getNumberOfAttempts()));
        } catch (InterruptedException i) {
            throw new RuntimeException(i);
        }
    }

    @VisibleForTesting
    boolean shouldBackoff(Exception ex, RequestExceptionHandlerStrategy strategy) {
        return strategy.shouldBackoff(ex);
    }

    private <K extends Exception> void handleRetryOnDifferentHosts(RetryableCassandraRequest<?, K> req,
            InetSocketAddress hostTried, Exception ex, RequestExceptionHandlerStrategy strategy) {
        if (shouldRetryOnDifferentHost(ex, req.getNumberOfAttempts(), strategy)) {
            log.info("Retrying with on a different host a query intended for host {}.",
                    SafeArg.of("hostName", CassandraLogHelper.host(hostTried)));
            req.giveUpOnPreferredHost();
        }
    }

    @VisibleForTesting
    boolean shouldRetryOnDifferentHost(Exception ex, int numberOfAttempts, RequestExceptionHandlerStrategy strategy) {
        return strategy.shouldRetryOnDifferentHost(ex, maxTriesSameHost.get(), numberOfAttempts);
    }

    // Determine the behavior we want from each type of exception.

    @VisibleForTesting
    static boolean isRetryable(Exception ex) {
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
                || isTransientException(ex.getCause()));
    }

    static boolean isIndicativeOfCassandraLoad(Throwable ex) {
        return ex != null
                // pool for this node is fully in use
                && (ex instanceof NoSuchElementException
                // Cassandra timeout. Maybe took too long to CAS, or Cassandra is under load.
                || ex instanceof TimedOutException
                // remote cassandra node couldn't talk to enough other remote cassandra nodes to answer
                || ex instanceof UnavailableException
                // Not enough Cassandra nodes are up.
                || ex instanceof InsufficientConsistencyException
                || isIndicativeOfCassandraLoad(ex.getCause()));
    }

    static boolean isFastFailoverException(Throwable ex) {
        return ex != null
                // underlying cassandra table does not exist. The table might exist on other cassandra nodes.
                && (ex instanceof InvalidRequestException
                || isFastFailoverException(ex.getCause()));
    }

    private static final String CONNECTION_FAILURE_MSG = "Tried to connect to cassandra {} times."
            + " Error writing to Cassandra socket."
            + " Likely cause: Exceeded maximum thrift frame size;"
            + " unlikely cause: network issues.";
    private static final String CONNECTION_FAILURE_MSG_WITH_EXCEPTION_INFO = CONNECTION_FAILURE_MSG
            + " Exception message was: {} : {}";

    @VisibleForTesting
    interface RequestExceptionHandlerStrategy {
        boolean shouldBackoff(Exception ex);
        long getBackoffPeriod(int numberOfAttempts);
        boolean shouldRetryOnDifferentHost(Exception ex, int maxTriesSameHost, int numberOfAttempts);
    }

    private static class Default implements RequestExceptionHandlerStrategy {
        private static final RequestExceptionHandlerStrategy  INSTANCE = new Default();

        private static final long BACKOFF_DURATION = Duration.ofSeconds(1).toMillis();

        @Override
        public boolean shouldBackoff(Exception ex) {
            return isConnectionException(ex) || isIndicativeOfCassandraLoad(ex);
        }

        @Override
        public long getBackoffPeriod(int numberOfAttempts) {
            // And value between -500 and +500ms to backoff to better spread load on failover
            return numberOfAttempts * BACKOFF_DURATION + (ThreadLocalRandom.current().nextInt(1000) - 500);
        }

        @Override
        public boolean shouldRetryOnDifferentHost(Exception ex, int maxTriesSameHost, int numberOfAttempts) {
            return isFastFailoverException(ex)
                    || (numberOfAttempts >= maxTriesSameHost
                    && (isConnectionException(ex) || isIndicativeOfCassandraLoad(ex)));
        }
    }

    private static class Conservative implements RequestExceptionHandlerStrategy {
        private static final RequestExceptionHandlerStrategy INSTANCE = new Conservative();
        private static final long MAX_BACKOFF = Duration.ofSeconds(30).toMillis();

        @Override
        public boolean shouldBackoff(Exception ex) {
            return !isFastFailoverException(ex);
        }

        @Override
        public long getBackoffPeriod(int numberOfAttempts) {
            return Math.min(500 * (long) Math.pow(2, numberOfAttempts), MAX_BACKOFF);
        }

        @Override
        public boolean shouldRetryOnDifferentHost(Exception ex, int maxTriesSameHost, int numberOfAttempts) {
            return isFastFailoverException(ex) || isIndicativeOfCassandraLoad(ex)
                    || (numberOfAttempts >= maxTriesSameHost && isConnectionException(ex));
        }
    }
}
