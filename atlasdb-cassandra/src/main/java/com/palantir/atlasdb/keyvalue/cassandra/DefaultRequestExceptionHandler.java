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
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

class DefaultRequestExceptionHandler extends AbstractRequestExceptionHandler {
    private static final Duration backoffDuration = Duration.ofSeconds(1);

    DefaultRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost,
            Supplier<Integer> maxTriesTotal,
            Blacklist blacklist) {
        super(maxTriesSameHost, maxTriesTotal, blacklist);
    }

    @Override
    <K extends Exception> void handleBackoff(RetryableCassandraRequest<?, K> req,
            InetSocketAddress hostTried,
            Exception ex) {
        if (!shouldBackoff(ex)) {
            return;
        }

        int numberOfAttempts = req.getNumberOfAttempts();
        long backoffPeriod = backoffDuration.toMillis();
        // And value between -500 and +500ms to backoff to better spread load on failover
        long sleepDuration =
                numberOfAttempts * backoffPeriod + (ThreadLocalRandom.current().nextInt(1000) - 500);
        log.info("Retrying a query, {}, with backoff of {}ms, intended for host {}.",
                UnsafeArg.of("queryString", req.getFunction().toString()),
                SafeArg.of("sleepDuration", sleepDuration),
                SafeArg.of("hostName", CassandraLogHelper.host(hostTried)));

        try {
            Thread.sleep(sleepDuration);
        } catch (InterruptedException i) {
            throw new RuntimeException(i);
        }
    }

    @Override
    <K extends Exception> void handleRetryOnDifferentHosts(RetryableCassandraRequest<?, K> req,
            InetSocketAddress hostTried, Exception ex) {
        if (shouldRetryOnDifferentHost(ex, req.getNumberOfAttempts())) {
            log.info("Retrying with on a different host a query intended for host {}.",
                    SafeArg.of("hostName", CassandraLogHelper.host(hostTried)));
            req.giveUpOnPreferredHost();
        }
    }

    @Override
    boolean shouldBlacklist(Exception ex, int numberOfAttempts) {
        return isConnectionException(ex) && numberOfAttempts >= maxTriesSameHost.get();
    }

    @VisibleForTesting
    boolean shouldBackoff(Exception ex) {
        return isConnectionException(ex) || isIndicativeOfCassandraLoad(ex);
    }

    @VisibleForTesting
    boolean shouldRetryOnDifferentHost(Exception ex, int numberOfAttempts) {
        return isFastFailoverException(ex)
                || (numberOfAttempts >= maxTriesSameHost.get()
                && (isConnectionException(ex) || isIndicativeOfCassandraLoad(ex)));
    }
}
