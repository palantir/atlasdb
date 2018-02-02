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

import java.util.function.Supplier;

class ConservativeRequestExceptionHandler extends AbstractRequestExceptionHandler {
    ConservativeRequestExceptionHandler(
            Supplier<Integer> maxTriesSameHost,
            Supplier<Integer> maxTriesTotal,
            Blacklist blacklist) {
        super(maxTriesSameHost, maxTriesTotal, blacklist);
    }

    @Override
    boolean shouldBackoff(Exception ex) {
        return !isFastFailoverException(ex);
    }

    @Override
    long getBackoffPeriod(int numberOfAttempts) {
        return 500 * (long) Math.pow(2, numberOfAttempts);
    }

    @Override
    boolean shouldRetryOnDifferentHost(Exception ex, int numberOfAttempts) {
        return isFastFailoverException(ex) || isIndicativeOfCassandraLoad(ex)
                || (numberOfAttempts >= maxTriesSameHost.get() && isConnectionException(ex));
    }
}

