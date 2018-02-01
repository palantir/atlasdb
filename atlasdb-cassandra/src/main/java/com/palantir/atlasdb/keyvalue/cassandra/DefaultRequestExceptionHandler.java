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

import com.palantir.logsafe.SafeArg;

class DefaultRequestExceptionHandler extends AbstractRequestExceptionHandler {
    private static final long backoffDuration = Duration.ofSeconds(1).toMillis();

    DefaultRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost,
            Supplier<Integer> maxTriesTotal,
            Blacklist blacklist) {
        super(maxTriesSameHost, maxTriesTotal, blacklist);
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

    @Override
    boolean shouldBackoff(Exception ex) {
        return isConnectionException(ex) || isIndicativeOfCassandraLoad(ex);
    }

    @Override
    long getBackoffPeriod(int numberOfAttempts) {
        // And value between -500 and +500ms to backoff to better spread load on failover
        return numberOfAttempts * backoffDuration + (ThreadLocalRandom.current().nextInt(1000) - 500);
    }

    @Override
    boolean shouldRetryOnDifferentHost(Exception ex, int numberOfAttempts) {
        return isFastFailoverException(ex)
                || (numberOfAttempts >= maxTriesSameHost.get()
                && (isConnectionException(ex) || isIndicativeOfCassandraLoad(ex)));
    }
}
