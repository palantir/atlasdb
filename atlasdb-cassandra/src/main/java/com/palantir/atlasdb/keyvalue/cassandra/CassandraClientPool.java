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

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.ClientCreationFailedException;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.concurrent.PTExecutors;

/**
 * Feature breakdown:
 *   - Pooling
 *   - Token Aware Mapping / Query Routing / Data partitioning
 *   - Retriable Queries
 *   - Pool member error tracking / blacklisting*
 *   - Pool refreshing
 *   - Pool node autodiscovery
 *   - Pool member health checking*
 *
 *   *entirely new features
 *
 *   By our old system, this would be a RefreshingRetriableTokenAwareHealthCheckingManyHostCassandraClientPoolingContainerManager;
 *   ... this is one of the reasons why there is a new system.
 **/
public class CassandraClientPool {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPool.class);
    /**
     * This is the maximum number of times we'll accept connection failures to one host before blacklisting it. Note
     * that subsequent hosts we try in the same call will actually be blacklisted after one connection failure
     */
    private static final int MAX_TRIES_SAME_HOST = 3;
    private static final int MAX_TRIES_TOTAL = 6;

    volatile RangeMap<LightweightOPPToken, List<InetSocketAddress>> tokenMap = ImmutableRangeMap.of();
    Map<InetSocketAddress, Long> blacklistedHosts = Maps.newConcurrentMap();
    Map<InetSocketAddress, CassandraClientPoolingContainer> currentPools = Maps.newConcurrentMap();
    final CassandraKeyValueServiceConfig config;
    final ScheduledThreadPoolExecutor refreshDaemon;


    public static class LightweightOPPToken implements Comparable<LightweightOPPToken> {
        final byte[] bytes;

        public LightweightOPPToken(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int compareTo(LightweightOPPToken other) {
            return UnsignedBytes.lexicographicalComparator().compare(this.bytes, other.bytes);
        }

        @Override
        public String toString() {
            return BaseEncoding.base16().encode(bytes);
        }
    }

    public CassandraClientPool(CassandraKeyValueServiceConfig config) {
        this.config = config;
        refreshDaemon = PTExecutors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CassandraClientPoolRefresh-%d").build());
        refreshDaemon.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    refreshPool();
                } catch (Throwable t) {
                    log.error("Failed to refresh Cassandra KVS pool. Extended periods of being unable to refresh will cause perf degradation.", t);
                }
            }
        }, config.poolRefreshIntervalSeconds(), config.poolRefreshIntervalSeconds(), TimeUnit.SECONDS);

        config.servers().forEach((server) -> currentPools.put(server, new CassandraClientPoolingContainer(server, config)));
        refreshPool(); // ensure we've initialized before returning
    }


    public void shutdown() {
        refreshDaemon.shutdown();
        currentPools.forEach((address, cassandraClientPoolingContainer) -> cassandraClientPoolingContainer.shutdownPooling());
    }

    private synchronized void refreshPool() {
        checkAndUpdateBlacklist();

        Set<InetSocketAddress> serversToAdd = Sets.newHashSet(config.servers());
        Set<InetSocketAddress> serversToRemove = ImmutableSet.of();

        if (config.autoRefreshNodes()) {
            refreshTokenRanges(); // re-use token mapping as list of hosts in the cluster
            for (List<InetSocketAddress> rangeOwners : tokenMap.asMapOfRanges().values()) {
                for (InetSocketAddress address : rangeOwners) {
                    serversToAdd.add(address);
                }
            }
        }

        serversToAdd = Sets.difference(serversToAdd, currentPools.keySet());

        if (!config.autoRefreshNodes()) { // (we would just add them back in)
            serversToRemove = Sets.difference(currentPools.keySet(), config.servers());
        }

        for (InetSocketAddress newServer : serversToAdd) {
            currentPools.put(newServer, new CassandraClientPoolingContainer(newServer, config));
        }

        for (InetSocketAddress removedServerAddress : serversToRemove) {
            removePool(removedServerAddress);
        }

        if (!(serversToAdd.isEmpty() && serversToRemove.isEmpty())) { // if we made any changes
            sanityCheckRingConsistency();
            if (!config.autoRefreshNodes()) { // grab new token mapping, if we didn't already do this before
                refreshTokenRanges();
            }
        }

        log.debug("Cassandra pool refresh added hosts {}, removed hosts {}.", serversToAdd, serversToRemove);
        debugLogStateOfPool();
    }

    private void removePool(InetSocketAddress removedServerAddress) {
        blacklistedHosts.remove(removedServerAddress);
        try {
            currentPools.get(removedServerAddress).shutdownPooling();
        } catch (Exception e) {
            log.warn("While removing a host ({}) from the pool, we were unable to gently cleanup resources.", removedServerAddress, e);
        }
        currentPools.remove(removedServerAddress);
    }

    private void debugLogStateOfPool() {
        if (log.isDebugEnabled()) {
            StringBuilder currentState = new StringBuilder();
            currentState.append(
                    String.format("POOL STATUS: Current blacklist = %s,\n current hosts in pool = %s\n",
                    blacklistedHosts.keySet().toString(), currentPools.keySet().toString()));
            for (Entry<InetSocketAddress, CassandraClientPoolingContainer> entry : currentPools.entrySet()) {
                int activeCheckouts = entry.getValue().getPoolUtilization();
                int totalAllowed = entry.getValue().getPoolSize();

                currentState.append(
                        String.format("\tPOOL STATUS: Pooled host %s has %s out of %s connections checked out.\n",
                                entry.getKey(),
                                activeCheckouts > 0? Integer.toString(activeCheckouts) : "(unknown)",
                                totalAllowed > 0? Integer.toString(totalAllowed) : "(not bounded)"));
            }
            log.debug(currentState.toString());
        }
    }

    private void checkAndUpdateBlacklist() {
        // Check blacklist and re-integrate or continue to wait as necessary
        for (Map.Entry<InetSocketAddress, Long> blacklistedEntry : blacklistedHosts.entrySet()) {
            long backoffTimeMillis = TimeUnit.SECONDS.toMillis(config.unresponsiveHostBackoffTimeSeconds());
            if (blacklistedEntry.getValue() + backoffTimeMillis < System.currentTimeMillis()) {
                InetSocketAddress host = blacklistedEntry.getKey();
                if (isHostHealthy(host)) {
                    blacklistedHosts.remove(host);
                    log.error("Added host {} back into the pool after a waiting period and successful health check.", host);
                }
            }
        }
    }

    private void addToBlacklist(InetSocketAddress badHost) {
        blacklistedHosts.put(badHost, System.currentTimeMillis());
        log.info("Blacklisted host '{}'", badHost);
    }

    private boolean isHostHealthy(InetSocketAddress host) {
        try {
            currentPools.get(host).runWithPooledResource(describeRing);
            return true;
        } catch (Exception e) {
            log.error("We tried to add {} back into the pool, but got an exception that caused to us distrust this host further.", host, e);
            return false;
        }
    }

    private CassandraClientPoolingContainer getRandomGoodHost() {
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = currentPools;

        Set<InetSocketAddress> livingHosts = Sets.difference(pools.keySet(), blacklistedHosts.keySet());
        if (livingHosts.isEmpty()) {
            log.error("There are no known live hosts in the connection pool. We're choosing one at random in a last-ditch attempt at forward progress.");
            livingHosts = pools.keySet();
        }

        return pools.get(getRandomHostByActiveConnections(Maps.filterKeys(currentPools, livingHosts::contains)));
    }

    public InetSocketAddress getRandomHostForKey(byte[] key) {
        List<InetSocketAddress> hostsForKey = tokenMap.get(new LightweightOPPToken(key));
        SetView<InetSocketAddress> liveOwnerHosts;

        if (hostsForKey == null) {
            log.warn("Cluster not fully initialized, not routing query to correct host as not token map found.");
            return getRandomGoodHost().getHost();
        } else {
            liveOwnerHosts = Sets.difference(ImmutableSet.copyOf(hostsForKey), blacklistedHosts.keySet());
        }

        if (liveOwnerHosts.isEmpty()) {
            log.warn("Perf / cluster stability issue. Token aware query routing has failed because there are no known " +
                    "live hosts that claim ownership of the given range. Falling back to choosing a random live node. " +
                    "Current state logged at DEBUG");
            log.debug("Current ring view is: {} and our current host blacklist is {}", tokenMap, blacklistedHosts);
            return getRandomGoodHost().getHost();
        } else {
            return getRandomHostByActiveConnections(Maps.filterKeys(currentPools, liveOwnerHosts::contains));
        }
    }

    private static InetSocketAddress getRandomHostByActiveConnections(Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
        return WeightedHosts.create(pools).getRandomHost();
    }

    public void runOneTimeStartupChecks() {
        final FunctionCheckedException<Cassandra.Client, Void, Exception> healthChecks = new FunctionCheckedException<Cassandra.Client, Void, Exception>() {
            @Override
            public Void apply(Cassandra.Client client) throws Exception {
                CassandraVerifier.validatePartitioner(client, config);
                return null;
            }
        };

        final FunctionCheckedException<Cassandra.Client, Void, Exception> createInternalMetadataTable = new FunctionCheckedException<Cassandra.Client, Void, Exception>() {
            @Override
            public Void apply(Cassandra.Client client) throws Exception {
                createTableInternal(client, CassandraConstants.METADATA_TABLE);
                return null;
            }
        };

        try {
            CassandraVerifier.ensureKeyspaceExistsAndIsUpToDate(this, config);

            for (InetSocketAddress host : currentPools.keySet()) {
                runOnHost(host, healthChecks);
                runOnHost(host, createInternalMetadataTable);
            }

        } catch (Exception e) {
            log.error("Startup checks failed.");
            throw new RuntimeException(e);
        }
    }

    //todo dedupe this into a name-demangling class that everyone can access
    protected static String internalTableName(String  tableName) {
        if (tableName.startsWith("_")) {
            return tableName;
        }
        return tableName.replaceFirst("\\.", "__");
    }

    // for tables internal / implementation specific to this KVS; these also don't get metadata in metadata table, nor do they show up in getTablenames
    private void createTableInternal(Client client, final String tableName) throws InvalidRequestException, SchemaDisagreementException, TException, NotFoundException {
        KsDef ks = client.describe_keyspace(config.keyspace());
        for (CfDef cf : ks.getCf_defs()) {
            if (cf.getName().equalsIgnoreCase(internalTableName(tableName))) {
                return;
            }
        }
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), internalTableName(tableName));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(client, tableName, config.schemaMutationTimeoutMillis());
        return;
    }

    private void refreshTokenRanges() {
        try {
            List<TokenRange> tokenRanges = getRandomGoodHost().runWithPooledResource(describeRing);

            ImmutableRangeMap.Builder<LightweightOPPToken, List<InetSocketAddress>> newTokenRing = ImmutableRangeMap.builder();
            for (TokenRange tokenRange : tokenRanges) {
                List<InetSocketAddress> hosts = Lists.transform(tokenRange.getEndpoints(), new Function<String, InetSocketAddress>() {
                    @Override
                    public InetSocketAddress apply(String endpoint) {
                            return new InetSocketAddress(endpoint, CassandraConstants.DEFAULT_THRIFT_PORT);
                    }
                });
                LightweightOPPToken startToken = new LightweightOPPToken(BaseEncoding.base16().decode(tokenRange.getStart_token().toUpperCase()));
                LightweightOPPToken endToken = new LightweightOPPToken(BaseEncoding.base16().decode(tokenRange.getEnd_token().toUpperCase()));
                if (startToken.compareTo(endToken) <= 0) {
                    newTokenRing.put(Range.openClosed(startToken, endToken), hosts);
                } else {
                    // Handle wrap-around
                    newTokenRing.put(Range.greaterThan(startToken), hosts);
                    newTokenRing.put(Range.atMost(endToken), hosts);
                }
            }
            tokenMap = newTokenRing.build();

        } catch (Exception e) {
            log.error("Couldn't grab new token ranges for token aware cassandra mapping!", e);
        }
    }

    private FunctionCheckedException<Cassandra.Client, List<TokenRange>, Exception> describeRing = new FunctionCheckedException<Cassandra.Client, List<TokenRange>, Exception>() {
        @Override
        public List<TokenRange> apply (Cassandra.Client client) throws Exception {
            return client.describe_ring(config.keyspace());
        }};

    public <V, K extends Exception> V runWithRetry(FunctionCheckedException<Cassandra.Client, V, K> f) throws K {
       return runWithRetryOnHost(getRandomGoodHost().getHost(), f);
    }

    public <V, K extends Exception> V runWithRetryOnHost(InetSocketAddress specifiedHost, FunctionCheckedException<Cassandra.Client, V, K> f) throws K {
        int numTries = 0;
        while (true) {
            CassandraClientPoolingContainer hostPool = currentPools.get(specifiedHost);

            if (blacklistedHosts.containsKey(specifiedHost) || hostPool == null) {
                log.warn("Randomly redirected a query intended for host {} because it was not currently a live member of the pool.", specifiedHost);
                hostPool = getRandomGoodHost();
            }

            try {
                return hostPool.runWithPooledResource(f);
            } catch (Exception e) {
                numTries++;
                this.<K>handleException(numTries, hostPool.getHost(), e);
            }
        }
    }

    public <V, K extends Exception> V run(FunctionCheckedException<Cassandra.Client, V, K> f) throws K {
        return runOnHost(getRandomGoodHost().getHost(), f);
    }

    private <V, K extends Exception> V runOnHost(InetSocketAddress specifiedHost,
                                                 FunctionCheckedException<Cassandra.Client, V, K> f) throws K {
        CassandraClientPoolingContainer hostPool = currentPools.get(specifiedHost);
        return hostPool.runWithPooledResource(f);
    }

        @SuppressWarnings("unchecked")
    private <K extends Exception> void handleException(int numTries, InetSocketAddress host, Exception e) throws K {
        if (isRetriableException(e)) {
            if (numTries >= MAX_TRIES_TOTAL) {
                if (e instanceof TTransportException
                        && e.getCause() != null
                        && (e.getCause().getClass() == SocketException.class)) {
                    String msg = "Error writing to Cassandra socket. Likely cause: Exceeded maximum thrift frame size; unlikely cause: network issues.";
                    log.error("Tried to connect to cassandra " + numTries + " times. " + msg, e);
                    e = new TTransportException(((TTransportException) e).getType(), msg, e);
                } else {
                    log.error("Tried to connect to cassandra " + numTries + " times.", e);
                }
                throw (K) e;
            } else {
                log.warn("Error occurred talking to cassandra. Attempt {} of {}.", numTries, MAX_TRIES_TOTAL, e);
                if (isConnectionException(e) && numTries >= MAX_TRIES_SAME_HOST) {
                    addToBlacklist(host);
                }
            }
        } else {
            throw (K) e;
        }
    }

    // This method exists to verify a particularly nasty bug where cassandra doesn't have a
    // consistent ring across all of it's nodes.  One node will think it owns more than the others
    // think it does and they will not send writes to it, but it will respond to requests
    // acting like it does.
    private void sanityCheckRingConsistency() {
        Multimap<Set<TokenRange>, InetSocketAddress> tokenRangesToHost = HashMultimap.create();
        for (InetSocketAddress host : currentPools.keySet()) {
            Cassandra.Client client = null;
            try {
                client = CassandraClientFactory.getClientInternal(host, config.ssl(), config.socketTimeoutMillis(), config.socketQueryTimeoutMillis());
                try {
                    client.describe_keyspace(config.keyspace());
                } catch (NotFoundException e) {
                    return; // don't care to check for ring consistency when we're not even fully initialized
                }
                tokenRangesToHost.put(ImmutableSet.copyOf(client.describe_ring(config.keyspace())), host);
            } catch (Exception e) {
                log.warn("failed to get ring info from host: {}", host, e);
            } finally {
                if (client != null) {
                    client.getOutputProtocol().getTransport().close();
                }
            }

            if (tokenRangesToHost.isEmpty()) {
                log.warn("Failed to get ring info for entire Cassandra cluster ({}); ring could not be checked for consistency.", config.keyspace());
                return;
            }

            if (tokenRangesToHost.keySet().size() == 1) { // all nodes agree on a consistent view of the cluster. Good.
                return;
            }

            RuntimeException e = new IllegalStateException("Hosts have differing ring descriptions.  This can lead to inconsistent reads and lost data. ");
            log.error("QA-86204 " + e.getMessage() + tokenRangesToHost, e);


            // provide some easier to grok logging for the two most common cases
            if (tokenRangesToHost.size() > 2) {
                for (Map.Entry<Set<TokenRange>, Collection<InetSocketAddress>> entry : tokenRangesToHost.asMap().entrySet()) {
                    if (entry.getValue().size() == 1) {
                        log.error("Host: " + entry.getValue().iterator().next() +
                                " disagrees with the other nodes about the ring state.");
                    }
                }
            }
            if (tokenRangesToHost.keySet().size() == 2) {
                ImmutableList<Set<TokenRange>> sets = ImmutableList.copyOf(tokenRangesToHost.keySet());
                Set<TokenRange> set1 = sets.get(0);
                Set<TokenRange> set2 = sets.get(1);
                log.error("Hosts are split.  group1: " + tokenRangesToHost.get(set1) +
                        " group2: " + tokenRangesToHost.get(set2));
            }

            CassandraVerifier.logErrorOrThrow(e.getMessage(), config.safetyDisabled());
        }
    }

    @VisibleForTesting
    static boolean isConnectionException(Throwable t) {
        return t != null
                && (t instanceof SocketTimeoutException
                || t instanceof ClientCreationFailedException
                || t instanceof UnavailableException
                || t instanceof NoSuchElementException
                || isConnectionException(t.getCause()));
    }

    @VisibleForTesting
    static boolean isRetriableException(Throwable t) {
        return t != null
                && (t instanceof TTransportException
                || t instanceof TimedOutException
                || t instanceof InsufficientConsistencyException
                || isConnectionException(t)
                || isRetriableException(t.getCause()));
    }

    /**
     * Weights hosts inversely by the number of active connections. {@link #getRandomHost()} should then be used to
     * pick a random host
     */
    @VisibleForTesting
    static class WeightedHosts {
        final TreeMap<Integer, InetSocketAddress> hosts;

        private WeightedHosts(TreeMap<Integer, InetSocketAddress> hosts) {
            this.hosts = hosts;
        }

        static WeightedHosts create(Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
            Preconditions.checkArgument(!pools.isEmpty(), "pools should be non-empty");
            return new WeightedHosts(buildHostsWeightedByActiveConnections(pools));
        }

        /**
         * The key for a host is the open upper bound of the weight. Since the domain is intended to be contiguous, the
         * closed lower bound of that weight is the key of the previous entry.
         * <p>
         * The closed lower bound of the first entry is 0.
         * <p>
         * Every weight is guaranteed to be non-zero in size. That is, every key is guaranteed to be at least one larger
         * than the previous key.
         */
        private static TreeMap<Integer, InetSocketAddress> buildHostsWeightedByActiveConnections(
                Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {

            Map<InetSocketAddress, Integer> activeConnectionsByHost = new HashMap<>(pools.size());
            int totalActiveConnections = 0;
            for (InetSocketAddress host : pools.keySet()) {
                int activeConnections = Math.max(pools.get(host).getPoolUtilization(), 0);
                activeConnectionsByHost.put(host, activeConnections);
                totalActiveConnections += activeConnections;
            }

            int lowerBoundInclusive = 0;
            TreeMap<Integer, InetSocketAddress> weightedHosts = new TreeMap<>();
            for (Entry<InetSocketAddress, Integer> entry : activeConnectionsByHost.entrySet()) {
                // We want the weight to be inversely proportional to the number of active connections so that we pick
                // less-active hosts. We add 1 to make sure that all ranges are non-empty
                int weight = totalActiveConnections - entry.getValue() + 1;
                weightedHosts.put(lowerBoundInclusive + weight, entry.getKey());
                lowerBoundInclusive += weight;
            }
            return weightedHosts;
        }

        InetSocketAddress getRandomHost() {
            int index = ThreadLocalRandom.current().nextInt(hosts.lastKey());
            return getRandomHostInternal(index);
        }

        // This basically exists for testing
        InetSocketAddress getRandomHostInternal(int index) {
            return hosts.higherEntry(index).getValue();
        }
    }
}
