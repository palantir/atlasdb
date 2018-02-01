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
import java.net.SocketTimeoutException;
import java.util.function.Supplier;

class CassandraRequestExceptionHandler {
    private final DefaultRequestExceptionHandler defaultRequestExceptionHandler;
    private final ConservativeRequestExceptionHandler conservativeRequestExceptionHandler;
    private final Supplier<Boolean> useConservativeHandler;

    CassandraRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost,
            Supplier<Integer> maxTriesTotal,
            Supplier<Boolean> useConservativeHandler,
            Blacklist blacklist) {
        this.defaultRequestExceptionHandler = new DefaultRequestExceptionHandler(
                maxTriesSameHost,
                maxTriesTotal,
                blacklist);
        this.conservativeRequestExceptionHandler = new ConservativeRequestExceptionHandler(
                maxTriesSameHost,
                maxTriesTotal,
                blacklist);
        this.useConservativeHandler = useConservativeHandler;
    }

    @SuppressWarnings("unchecked")
    <K extends Exception> void handleExceptionFromRequest(
                RetryableCassandraRequest<?, K> req,
                InetSocketAddress hostTried,
                Exception ex)
            throws K {
        if (useConservativeHandler.get()) {
            conservativeRequestExceptionHandler.handleExceptionFromRequest(req, hostTried, ex);
        } else {
            defaultRequestExceptionHandler.handleExceptionFromRequest(req, hostTried, ex);
        }
    }

    static boolean isConnectionException(Throwable ex) {
        return ex != null
                // tcp socket timeout, possibly indicating network flake, long GC, or restarting server.
                && (ex instanceof SocketTimeoutException
                || ex instanceof CassandraClientFactory.ClientCreationFailedException
                || isConnectionException(ex.getCause()));
    }

}
