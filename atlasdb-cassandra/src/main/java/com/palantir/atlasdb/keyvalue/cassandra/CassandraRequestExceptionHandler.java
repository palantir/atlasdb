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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.cassandra.pool.DcAwareHost;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;

class CassandraRequestExceptionHandler {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraRequestExceptionHandler.class);

    private final Supplier<Integer> maxTriesSameHost;
    private final Supplier<Integer> maxTriesTotal;
    private final Supplier<Boolean> useConservativeHandler;
    private final Blacklist blacklist;
    private final Optional<RequestExceptionHandlerStrategy> overrideStrategyForTest;

    CassandraRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost,
            Supplier<Integer> maxTriesTotal,
            Supplier<Boolean> useConservativeHandler,
            Blacklist blacklist) {
        this.maxTriesSameHost = maxTriesSameHost;
        this.maxTriesTotal = maxTriesTotal;
        this.useConservativeHandler = useConservativeHandler;
        this.blacklist = blacklist;
        this.overrideStrategyForTest = Optional.empty();
    }

    private CassandraRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost, Supplier<Integer> maxTriesTotal, Blacklist blacklist) {
        this.maxTriesSameHost = maxTriesSameHost;
        this.maxTriesTotal = maxTriesTotal;
        this.useConservativeHandler = () -> true;
        this.blacklist = blacklist;
        this.overrideStrategyForTest = Optional.of(NoBackoffForTesting.INSTANCE);
    }

    static CassandraRequestExceptionHandler withNoBackoffForTest(
            Supplier<Integer> maxTriesSameHost, Supplier<Integer> maxTriesTotal, Blacklist blacklist) {
        return new CassandraRequestExceptionHandler(maxTriesSameHost, maxTriesTotal, blacklist);
    }

    @SuppressWarnings("unchecked")
    <K extends Exception> void handleExceptionFromRequest(
            RetryableCassandraRequest<?, K> req, DcAwareHost hostTried, Exception ex) throws K {
        if (!isRetryable(ex)) {
            throw (K) ex;
        }

        RequestExceptionHandlerStrategy strategy = getStrategy();

        req.triedOnHost(hostTried);
        req.registerException(ex);
        int numberOfAttempts = req.getNumberOfAttempts();
        int numberOfAttemptsOnHost = req.getNumberOfAttemptsOnHost(hostTried);

        if (numberOfAttempts >= maxTriesTotal.get()) {
            throw logAndThrowException(numberOfAttempts, ex, req);
        }

        if (shouldBlacklist(ex, numberOfAttemptsOnHost)) {
            blacklist.add(hostTried);
        }

        logNumberOfAttempts(ex, numberOfAttempts);
        handleBackoff(req, hostTried, ex, strategy);
        handleRetryOnDifferentHosts(req, hostTried, ex, strategy);
    }

    @VisibleForTesting
    RequestExceptionHandlerStrategy getStrategy() {
        return overrideStrategyForTest.orElseGet(this::resolveStrategy);
    }

    private RequestExceptionHandlerStrategy resolveStrategy() {
        if (useConservativeHandler.get()) {
            return Conservative.INSTANCE;
        } else {
            return LegacyExceptionHandler.INSTANCE;
        }
    }

    private static AtlasDbDependencyException logAndThrowException(
            int numberOfAttempts, Exception ex, RetryableCassandraRequest<?, ?> req) {
        log.warn(
                "Tried to connect to cassandra {} times. Exception message was: {} : {}",
                SafeArg.of("numTries", numberOfAttempts),
                SafeArg.of("exceptionClass", ex.getClass().getTypeName()),
                UnsafeArg.of("exceptionMessage", ex.getMessage()));
        throw req.throwLimitReached();
    }

    private void logNumberOfAttempts(Exception ex, int numberOfAttempts) {
        // Only log the actual exception the first time
        if (numberOfAttempts > 1) {
            log.debug(
                    "Error occurred talking to cassandra. Attempt {} of {}. Exception message was: {} : {}",
                    SafeArg.of("numTries", numberOfAttempts),
                    SafeArg.of("maxTotalTries", maxTriesTotal.get()),
                    SafeArg.of("exceptionClass", ex.getClass().getTypeName()),
                    UnsafeArg.of("exceptionMessage", ex.getMessage()));
        } else {
            log.debug(
                    "Error occurred talking to cassandra. Attempt {} of {}.",
                    SafeArg.of("numTries", numberOfAttempts),
                    SafeArg.of("maxTotalTries", maxTriesTotal.get()),
                    ex);
        }
    }

    @VisibleForTesting
    boolean shouldBlacklist(Exception ex, int numberOfAttempts) {
        return isConnectionException(ex)
                && numberOfAttempts >= maxTriesSameHost.get()
                && !isExceptionNotImplicatingThisParticularNode(ex);
    }

    private <K extends Exception> void handleBackoff(
            RetryableCassandraRequest<?, K> req,
            DcAwareHost hostTried,
            Exception ex,
            RequestExceptionHandlerStrategy strategy) {
        if (!shouldBackoff(ex, strategy)) {
            return;
        }

        long backOffPeriod = strategy.getBackoffPeriod(req.getNumberOfAttemptsOnHost(hostTried));
        log.info(
                "Retrying a query, {}, with backoff of {}ms, intended for host {}.",
                UnsafeArg.of("queryString", req.getFunction().toString()),
                SafeArg.of("sleepDuration", backOffPeriod),
                SafeArg.of("hostName", CassandraLogHelper.host(hostTried)));

        try {
            Thread.sleep(backOffPeriod);
        } catch (InterruptedException i) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(i);
        }
    }

    @VisibleForTesting
    boolean shouldBackoff(Exception ex, RequestExceptionHandlerStrategy strategy) {
        return strategy.shouldBackoff(ex);
    }

    private <K extends Exception> void handleRetryOnDifferentHosts(
            RetryableCassandraRequest<?, K> req,
            DcAwareHost hostTried,
            Exception ex,
            RequestExceptionHandlerStrategy strategy) {
        if (shouldRetryOnDifferentHost(ex, req.getNumberOfAttemptsOnHost(hostTried), strategy)) {
            log.info(
                    "Retrying a query intended for host {} on a different host.",
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
                && (ex instanceof TTransportException || isTransientException(ex.getCause()));
    }

    static boolean isIndicativeOfCassandraLoad(Throwable ex) {
        // TODO (jkong): Make NoSuchElementException its own thing - that is NOT necessarily indicative of C* load.
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
                && (ex instanceof InvalidRequestException || isFastFailoverException(ex.getCause()));
    }

    static boolean isExceptionNotImplicatingThisParticularNode(Throwable ex) {
        // client pool has no more elements to give, so this service node has too many requests in flight.
        return ex instanceof NoSuchElementException
                // Other nodes are down. Technically this node might be isolated from a functional quorum via a network
                // partition but we consider that to be an edge case.
                || ex instanceof InsufficientConsistencyException
                || isIndicativeOfCassandraLoad(ex.getCause());
    }

    @VisibleForTesting
    interface RequestExceptionHandlerStrategy {
        boolean shouldBackoff(Exception ex);

        long getBackoffPeriod(int numberOfAttempts);

        /**
         * Exceptions that cause a host to be blacklisted shouldn't be retried on another host. As number of
         * retries are recorded per request, and we want the number of retries on that specific host to exceed a
         * determined value before blacklisting.
         */
        boolean shouldRetryOnDifferentHost(Exception ex, int maxTriesSameHost, int numberOfAttempts);
    }

    private static final class LegacyExceptionHandler implements RequestExceptionHandlerStrategy {
        private static final RequestExceptionHandlerStrategy INSTANCE = new LegacyExceptionHandler();

        private static final long BACKOFF_DURATION = Duration.ofSeconds(1).toMillis();
        private static final int FIVE_HUNDRED = 500;

        @Override
        public boolean shouldBackoff(Exception ex) {
            return isConnectionException(ex) || isIndicativeOfCassandraLoad(ex);
        }

        @Override
        public long getBackoffPeriod(int numberOfAttempts) {
            // And value between -500 and +500ms to backoff to better spread load on failover
            return numberOfAttempts * BACKOFF_DURATION
                    + (ThreadLocalRandom.current().nextInt(2 * FIVE_HUNDRED) - FIVE_HUNDRED);
        }

        @Override
        public boolean shouldRetryOnDifferentHost(Exception ex, int maxTriesSameHost, int numberOfAttempts) {
            return isFastFailoverException(ex) || numberOfAttempts >= maxTriesSameHost;
        }
    }

    private static class Conservative implements RequestExceptionHandlerStrategy {
        private static final RequestExceptionHandlerStrategy INSTANCE = new Conservative();
        private static final long MAX_BACKOFF = Duration.ofSeconds(30).toMillis();
        private static final double UNCERTAINTY = 0.5;
        private static final double BACKOFF_COEFF = 100;

        private static final long MAX_EXPECTED_BACKOFF = (long) (MAX_BACKOFF / (UNCERTAINTY + 1));

        @Override
        public boolean shouldBackoff(Exception ex) {
            return !isFastFailoverException(ex);
        }

        @Override
        public long getBackoffPeriod(int numberOfAttempts) {
            double randomCoeff = ThreadLocalRandom.current().nextDouble(1 - UNCERTAINTY, 1 + UNCERTAINTY);
            return (long) (Math.min(BACKOFF_COEFF * Math.pow(2, numberOfAttempts), MAX_EXPECTED_BACKOFF) * randomCoeff);
        }

        @Override
        public boolean shouldRetryOnDifferentHost(Exception ex, int maxTriesSameHost, int numberOfAttempts) {
            return isFastFailoverException(ex)
                    || isIndicativeOfCassandraLoad(ex)
                    || numberOfAttempts >= maxTriesSameHost;
        }
    }

    private static final class NoBackoffForTesting extends Conservative {
        private static final RequestExceptionHandlerStrategy INSTANCE = new NoBackoffForTesting();

        @Override
        public long getBackoffPeriod(int numberOfAttempts) {
            return 0;
        }
    }
}
