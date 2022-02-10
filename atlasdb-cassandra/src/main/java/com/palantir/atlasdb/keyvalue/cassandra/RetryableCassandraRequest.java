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

import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.atlasdb.keyvalue.cassandra.pool.DcAwareHost;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.exception.AtlasDbDependencyException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RetryableCassandraRequest<V, K extends Exception> {
    private final DcAwareHost preferredHost;
    private final FunctionCheckedException<CassandraClient, V, K> fn;

    private boolean shouldGiveUpOnPreferredHost = false;
    private Map<DcAwareHost, Integer> triedHosts = new ConcurrentHashMap<>();
    private List<Exception> encounteredExceptions = new ArrayList<>();

    public RetryableCassandraRequest(DcAwareHost preferredHost, FunctionCheckedException<CassandraClient, V, K> fn) {
        this.preferredHost = preferredHost;
        this.fn = fn;
    }

    public DcAwareHost getPreferredHost() {
        return preferredHost;
    }

    public FunctionCheckedException<CassandraClient, V, K> getFunction() {
        return fn;
    }

    public int getNumberOfAttempts() {
        return triedHosts.values().stream().mapToInt(Number::intValue).sum();
    }

    public int getNumberOfAttemptsOnHost(DcAwareHost host) {
        return triedHosts.getOrDefault(host, 0);
    }

    public boolean shouldGiveUpOnPreferredHost() {
        return shouldGiveUpOnPreferredHost;
    }

    public void giveUpOnPreferredHost() {
        shouldGiveUpOnPreferredHost = true;
    }

    public boolean alreadyTriedOnHost(DcAwareHost address) {
        return triedHosts.containsKey(address);
    }

    public void triedOnHost(DcAwareHost host) {
        triedHosts.merge(host, 1, (old, ignore) -> old + 1);
    }

    public void registerException(Exception exception) {
        encounteredExceptions.add(exception);
    }

    public AtlasDbDependencyException throwLimitReached() {
        throw new RetryLimitReachedException(encounteredExceptions);
    }
}
