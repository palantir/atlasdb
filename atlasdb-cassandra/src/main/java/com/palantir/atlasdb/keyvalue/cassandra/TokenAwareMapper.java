/**
 * Copyright 2015 Palantir Technologies
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.TokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.pooling.PoolingContainer;

/**
 * Keep track of which Cassandra nodes own which data ranges, for the purpose of steering queries nicely / batching such
 * that we more often talk to coordinators who are also owners of the relevant data.
 *
 * If someone really cares, and wants to pretend this is an interview question, this might be pleasantly collapsable
 * into a custom Interval || Segment Tree.
 *
 * The whole thing is hilarious anyways, because Cassandra herself does a dumb linear traversal right now for the
 * server-side version of this, along with a 'todo: maybe we should be smarter about this', which I'll probably fix up
 * in a patch.
 *
 * @author clockfort
 */
public class TokenAwareMapper {
    private static final Logger log = LoggerFactory.getLogger(TokenAwareMapper.class);

    private final PoolingContainer<Client> clientPool;
    private final CassandraKeyValueServiceConfigManager configManager;

    private final AtomicReference<RangeMap<Token, List<InetAddress>>> tokenRing = new AtomicReference<RangeMap<Token, List<InetAddress>>>();

    private final Random random = new Random();

    private final ScheduledExecutorService ringRefreshExecutor =
            PTExecutors.newScheduledThreadPool(1, new NamedThreadFactory("CassandraRingRefreshThread", true));

    private class Token implements Comparable<Token> {
        byte[] bytes;

        public Token(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int compareTo(Token other) {
            return UnsignedBytes.lexicographicalComparator().compare(this.bytes, other.bytes);
        }
    }

    private TokenAwareMapper(CassandraKeyValueServiceConfigManager configManager, PoolingContainer<Client> clientPool) {
        this.clientPool = clientPool;
        this.configManager = configManager;
    }

    /**
     * Creates a {@link TokenAwareMapper}
     * <p>
     * The keyspace returned by {@link CassandraKeyValueServiceConfig#keyspace()} must already exist
     */
    public static TokenAwareMapper create(CassandraKeyValueServiceConfigManager configManager, PoolingContainer<Client> clientPool) {
        TokenAwareMapper tokenAwareMapper = new TokenAwareMapper(configManager, clientPool);
        tokenAwareMapper.refresh();
        tokenAwareMapper.scheduleRefreshTask();
        return tokenAwareMapper;
    }

    public void refresh() {
        List<TokenRange> tokenRanges = getTokenRanges();

        ImmutableRangeMap.Builder<Token, List<InetAddress>> newTokenRing = ImmutableRangeMap.builder();
        for (TokenRange tokenRange : tokenRanges) {
            List<InetAddress> hosts = Lists.transform(tokenRange.getEndpoints(), new Function<String, InetAddress>() {
                @Override
                public InetAddress apply(String endpoint) {
                    try {
                        return InetAddress.getByName(endpoint);
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            Token startToken = new Token(BaseEncoding.base16().decode(tokenRange.getStart_token().toUpperCase()));
            Token endToken = new Token(BaseEncoding.base16().decode(tokenRange.getEnd_token().toUpperCase()));
            if (startToken.compareTo(endToken) <= 0) {
                newTokenRing.put(Range.openClosed(startToken, endToken), hosts);
            } else {
                // Handle wrap-around
                newTokenRing.put(Range.greaterThan(startToken), hosts);
                newTokenRing.put(Range.atMost(endToken), hosts);
            }
        }
        tokenRing.set(newTokenRing.build());
    }

    private void scheduleRefreshTask() {
        ringRefreshExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                refresh();
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    private List<TokenRange> getTokenRanges() {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        try {
            return clientPool.runWithPooledResource(new FunctionCheckedException<Client, List<TokenRange>, Exception>() {
                @Override
                public List<TokenRange> apply(Client client) throws Exception {
                    return client.describe_ring(config.keyspace());
                }
            });
        } catch (Exception e) {
            log.error("Couldn't grab new token ranges for token aware cassandra mapping!", e);
            throw Throwables.propagate(e);
        }
    }

    public InetAddress getRandomHostForKey(byte[] key) {
        List<InetAddress> owners = tokenRing.get().get(new Token(key));
        return owners.get(random.nextInt(owners.size()));
    }

    public void shutdown() {
        ringRefreshExecutor.shutdown();
    }
}
