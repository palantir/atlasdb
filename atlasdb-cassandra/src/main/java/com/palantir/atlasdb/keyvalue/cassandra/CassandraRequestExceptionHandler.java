/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.ClientCreationFailedException;
import com.palantir.atlasdb.qos.ratelimit.RateLimitExceededException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

class CassandraRequestExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPool.class);

    private final Supplier<Integer> maxTriesSameHost;
    private final Supplier<Integer> maxTriesTotal;
    private final Blacklist blacklist;

    CassandraRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost,
            Supplier<Integer> maxTriesTotal,
            Blacklist blacklist) {
        this.maxTriesSameHost = maxTriesSameHost;
        this.maxTriesTotal = maxTriesTotal;
        this.blacklist = blacklist;
    }

    <K extends Exception> void handleExceptionFromRequest(
                RetryableCassandraRequest<?, K> req,
                InetSocketAddress hostTried,
                Exception ex)
            throws K {
        req.triedOnHost(hostTried);
        this.handleNumberOfAttempts(req.getNumberOfAttempts(), hostTried, ex);

        if (isRetriableWithBackoffException(ex)) {
            // And value between -500 and +500ms to backoff to better spread load on failover
            int sleepDuration =
                    req.getNumberOfAttempts() * 1000 + (ThreadLocalRandom.current().nextInt(1000) - 500);
            log.info("Retrying a query, {}, with backoff of {}ms, intended for host {}.",
                    UnsafeArg.of("queryString", req.getFunction().toString()),
                    SafeArg.of("sleepDuration", sleepDuration),
                    SafeArg.of("hostName", CassandraLogHelper.host(hostTried)));

            try {
                Thread.sleep(sleepDuration);
            } catch (InterruptedException i) {
                throw new RuntimeException(i);
            }
            if (req.getNumberOfAttempts() >= maxTriesSameHost.get()) {
                req.giveUpOnPreferredHost();
            }
        } else if (isFastFailoverException(ex)) {
            log.info("Retrying with fast failover a query intended for host {}.",
                    SafeArg.of("hostName", CassandraLogHelper.host(hostTried)));
            req.giveUpOnPreferredHost();
        }
    }

    @SuppressWarnings("unchecked")
    private <K extends Exception> void handleNumberOfAttempts(int numberOfAttempts,
            InetSocketAddress host,
            Exception ex) throws K {
        if (!isRetryable(ex)) {
            throw (K) ex;
        }

        if (numberOfAttempts >= maxTriesTotal.get()) {
            throw (K) logAndReturnException(numberOfAttempts, ex);
        }

        if (isConnectionException(ex) && numberOfAttempts >= maxTriesSameHost.get()) {
            blacklist.add(host);
        }

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

    @SuppressWarnings("unchecked")
    private <K extends Exception> K logAndReturnException(int numberOfAttempts, Exception ex) {
        if (ex instanceof TTransportException
                && ex.getCause() != null
                && (ex.getCause().getClass() == SocketException.class)) {
            log.error(CONNECTION_FAILURE_MSG, numberOfAttempts, ex);
            String errorMsg =
                    MessageFormatter.format(CONNECTION_FAILURE_MSG, numberOfAttempts).getMessage();
            return (K) new TTransportException(((TTransportException) ex).getType(), errorMsg, ex);
        } else {
            log.error("Tried to connect to cassandra {} times.",
                    SafeArg.of("numTries", numberOfAttempts), ex);
            return (K) ex;
        }
    }

    private boolean isRetryable(Exception ex) {
        return isRetriableException(ex)
                || isRetriableWithBackoffException(ex)
                || isFastFailoverException(ex)
                || isQosException(ex);
    }

    @VisibleForTesting
    static boolean isConnectionException(Throwable ex) {
        return ex != null
                && (ex instanceof SocketTimeoutException
                || ex instanceof ClientCreationFailedException
                || isConnectionException(ex.getCause()));
    }

    @VisibleForTesting
    static boolean isRetriableException(Throwable ex) {
        return ex != null
                && (ex instanceof TTransportException
                || ex instanceof TimedOutException
                || ex instanceof InsufficientConsistencyException
                || isConnectionException(ex)
                || isRetriableException(ex.getCause()));
    }

    @VisibleForTesting
    static boolean isRetriableWithBackoffException(Throwable ex) {
        return ex != null
                // pool for this node is fully in use
                && (ex instanceof NoSuchElementException
                // remote cassandra node couldn't talk to enough other remote cassandra nodes to answer
                || ex instanceof UnavailableException
                // tcp socket timeout, possibly indicating network flake, long GC, or restarting server
                || isConnectionException(ex)
                || isRetriableWithBackoffException(ex.getCause()));
    }

    @VisibleForTesting
    static boolean isFastFailoverException(Throwable ex) {
        return ex != null
                // underlying cassandra table does not exist. The table might exist on other cassandra nodes.
                && (ex instanceof InvalidRequestException
                || isFastFailoverException(ex.getCause()));
    }

    @VisibleForTesting
    static boolean isQosException(Throwable ex) {
        return ex != null
                && (ex instanceof RateLimitExceededException
                || isQosException(ex.getCause()));
    }

    private static final String CONNECTION_FAILURE_MSG = "Tried to connect to cassandra {} times."
            + " Error writing to Cassandra socket."
            + " Likely cause: Exceeded maximum thrift frame size;"
            + " unlikely cause: network issues.";
}
