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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.TokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolImpl;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraLogHelper;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraUtils;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.keyvalue.cassandra.TokenRangeWritesLogger;
import com.palantir.common.base.Throwables;

public class CassandraService {
    // TODO(tboam): keep logging on old class?
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPool.class);

    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPoolImpl pool;

    @VisibleForTesting
    volatile RangeMap<LightweightOppToken, List<InetSocketAddress>> tokenMap = ImmutableRangeMap.of();
    private final TokenRangeWritesLogger tokenRangeWritesLogger = TokenRangeWritesLogger.createUninitialized();

    public CassandraService(CassandraKeyValueServiceConfig config, CassandraClientPoolImpl cassandraClientPool) {
        this.config = config;
        this.pool = cassandraClientPool;
    }

    public TokenRangeWritesLogger getTokenRangeWritesLogger() {
        return tokenRangeWritesLogger;
    }

    public Set<InetSocketAddress> refreshTokenRanges() {
        Set<InetSocketAddress> servers = Sets.newHashSet();

        try {
            ImmutableRangeMap.Builder<LightweightOppToken, List<InetSocketAddress>> newTokenRing =
                    ImmutableRangeMap.builder();

            // grab latest token ring view from a random node in the cluster
            List<TokenRange> tokenRanges = getTokenRanges();

            // RangeMap needs a little help with weird 1-node, 1-vnode, this-entire-feature-is-useless case
            if (tokenRanges.size() == 1) {
                String onlyEndpoint = Iterables.getOnlyElement(Iterables.getOnlyElement(tokenRanges).getEndpoints());
                InetSocketAddress onlyHost = pool.getAddressForHost(onlyEndpoint);
                newTokenRing.put(Range.all(), ImmutableList.of(onlyHost));
            } else { // normal case, large cluster with many vnodes
                for (TokenRange tokenRange : tokenRanges) {
                    List<InetSocketAddress> hosts = tokenRange.getEndpoints().stream()
                            .map(host -> getAddressForHostThrowUnchecked(host)).collect(Collectors.toList());

                    servers.addAll(hosts);

                    LightweightOppToken startToken = new LightweightOppToken(
                            BaseEncoding.base16().decode(tokenRange.getStart_token().toUpperCase()));
                    LightweightOppToken endToken = new LightweightOppToken(
                            BaseEncoding.base16().decode(tokenRange.getEnd_token().toUpperCase()));
                    if (startToken.compareTo(endToken) <= 0) {
                        newTokenRing.put(Range.openClosed(startToken, endToken), hosts);
                    } else {
                        // Handle wrap-around
                        newTokenRing.put(Range.greaterThan(startToken), hosts);
                        newTokenRing.put(Range.atMost(endToken), hosts);
                    }
                }
            }
            tokenMap = newTokenRing.build();

            tokenRangeWritesLogger.updateTokenRanges(tokenMap.asMapOfRanges().keySet());
            return servers;
        } catch (Exception e) {
            log.error("Couldn't grab new token ranges for token aware cassandra mapping!", e);

            // return the set of servers we knew about last time we successfully constructed the tokenMap
            return tokenMap.asMapOfRanges().values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        }
    }

    private List<TokenRange> getTokenRanges() throws Exception {
        return pool.getRandomGoodHost().runWithPooledResource(CassandraUtils.getDescribeRing(config));
    }

    private InetSocketAddress getAddressForHostThrowUnchecked(String host) {
        try {
            return pool.getAddressForHost(host);
        } catch (UnknownHostException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public List<InetSocketAddress> getHostsFor(byte[] key) {
        return tokenMap.get(new LightweightOppToken(key));
    }

    public String getRingViewDescription() {
        return CassandraLogHelper.tokenMap(tokenMap).toString();
    }

    public RangeMap<LightweightOppToken, List<InetSocketAddress>> getTokenMap() {
        return tokenMap;
    }

    public <V> void markWritesForTable(Map<Cell, V> entries, TableReference tableRef) {
        tokenRangeWritesLogger.markWritesForTable(entries.keySet(), tableRef);
    }
}
