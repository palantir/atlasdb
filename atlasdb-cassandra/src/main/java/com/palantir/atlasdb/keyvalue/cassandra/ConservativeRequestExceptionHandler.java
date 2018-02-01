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
import java.util.function.Supplier;

class ConservativeRequestExceptionHandler extends AbstractRequestExceptionHandler {
    ConservativeRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost,
            Supplier<Integer> maxTriesTotal,
            Blacklist blacklist) {
        super(maxTriesSameHost, maxTriesTotal, blacklist);
    }

    @Override
    boolean shouldBlacklist(Exception ex, int numberOfAttempts) {
        // TODO
        return false;
    }

    @Override
    <K extends Exception> void handleBackoff(RetryableCassandraRequest<?, K> req, InetSocketAddress hostTried,
            Exception ex) {
        // TODO
    }

    @Override
    <K extends Exception> void handleRetryOnDifferentHosts(RetryableCassandraRequest<?, K> req,
            InetSocketAddress hostTried, Exception ex) {
        // TODO
    }
}

