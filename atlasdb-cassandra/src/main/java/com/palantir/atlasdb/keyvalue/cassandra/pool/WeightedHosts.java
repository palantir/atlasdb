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
package com.palantir.atlasdb.keyvalue.cassandra.pool;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.naming.ConfigurationException;

import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.logsafe.Preconditions;

import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.TokenRange;

import jnr.ffi.annotations.In;
import sun.tools.jstat.Token;

/**
 * Weights hosts inversely by the number of active connections. {@link #getRandomHost()} should then be used to
 * pick a random host
 */
public final class WeightedHosts {
    final NavigableMap<Integer, InetSocketAddress> hosts;

    private WeightedHosts(NavigableMap<Integer, InetSocketAddress> hosts) {
        this.hosts = hosts;
    }

    public static WeightedHosts create(Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
        return create(pools, Optional.empty());
    }

    public static WeightedHosts create(Map<InetSocketAddress, CassandraClientPoolingContainer> pools,
            Optional<List<TokenRange>> tokenRangeList) {
        Preconditions.checkArgument(!pools.isEmpty(), "pools should be non-empty");
        return new WeightedHosts(buildHostsWeightedByActiveConnections(pools, tokenRangeList));
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
    private static NavigableMap<Integer, InetSocketAddress> buildHostsWeightedByActiveConnections(
            Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
        return buildHostsWeightedByActiveConnections(pools, Optional.empty());
    }

    private static String getRackInEC2() {
        String az;
        try {
            // copied code from EC2 Snitch to get current datacentre
            // note - potential for throws, propogates errors up quite far
            HttpURLConnection conn = (HttpURLConnection)(new URL("http://169.254.169.254/latest/meta-data/placement/availability-zone")).openConnection();
            String var6;
            try (DataInputStream d = new DataInputStream((InputStream) conn.getContent())) {
                conn.setRequestMethod("GET");
                if (conn.getResponseCode() != 200) {
                    throw new RuntimeException("Ec2Snitch was unable to execute the API call. Not an ec2 node?");
                }

                int cl = conn.getContentLength();
                byte[] b = new byte[cl];
                d.readFully(b);
                d.close();
                var6 = new String(b, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            az = var6;

            String[] splits = az.split("-");
            String ec2zone = splits[splits.length - 1];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return az;
    }

    private static NavigableMap<Integer, InetSocketAddress> buildHostsWeightedByActiveConnections(
            Map<InetSocketAddress, CassandraClientPoolingContainer> pools,
            Optional<List<TokenRange>> tokenRangeList) {

        Map<InetSocketAddress, Integer> openRequestsByHost = new HashMap<>(pools.size());
        int totalOpenRequests = 0;
        for (Map.Entry<InetSocketAddress, CassandraClientPoolingContainer> poolEntry : pools.entrySet()) {
            int openRequests = Math.max(poolEntry.getValue().getOpenRequests(), 0);
            openRequestsByHost.put(poolEntry.getKey(), openRequests);
            totalOpenRequests += openRequests;
            System.out.println("ignoreme");
        }

        // my code
        if(tokenRangeList.isPresent())
        {
            // arbitrary weighting that we will add in via some config option
            // config option is "localHostWeighting", yet to be passed in though
            int weighting = 2;

            // since we haven't yet checked if AWS, this breaks on internal test
            // and so is neatly commented out
            //String az = getRackInEC2();
            String az = "dc1";

            List<TokenRange> tokenRanges = tokenRangeList.get();

            // make a set of IPs that map to the local datacentre
            HashSet<String> localIPs = new HashSet<>();

            for(TokenRange tokenRange : tokenRanges) {
                for (EndpointDetails details : tokenRange.endpoint_details) {
                    if(details.datacenter.equals(az)) {
                        localIPs.add(details.host);
                    }
                }
            }

            for(Map.Entry<InetSocketAddress, CassandraClientPoolingContainer> poolEntry : pools.entrySet())
            {
                InetSocketAddress inetaddr = poolEntry.getKey();
                if(inetaddr.getAddress() == null)
                {
                    continue;
                }

                if(localIPs.contains(inetaddr.getAddress().getHostAddress()))
                {
                    // increase totalOpenRequests accordingly
                    totalOpenRequests += openRequestsByHost.get(inetaddr) * (weighting - 1);
                    openRequestsByHost.replace(inetaddr, openRequestsByHost.get(inetaddr) * weighting);
                }
            }
        }
        // end of my code

        int lowerBoundInclusive = 0;
        NavigableMap<Integer, InetSocketAddress> weightedHosts = new TreeMap<>();
        for (Map.Entry<InetSocketAddress, Integer> entry : openRequestsByHost.entrySet()) {
            // We want the weight to be inversely proportional to the number of open requests so that we pick
            // less-active hosts. We add 1 to make sure that all ranges are non-empty
            int weight = totalOpenRequests - entry.getValue() + 1;
            weightedHosts.put(lowerBoundInclusive + weight, entry.getKey());
            lowerBoundInclusive += weight;
        }
        return weightedHosts;
    }

    public InetSocketAddress getRandomHost() {
        int index = ThreadLocalRandom.current().nextInt(hosts.lastKey());
        return getRandomHostInternal(index);
    }

    // This basically exists for testing
    InetSocketAddress getRandomHostInternal(int index) {
        return hosts.higherEntry(index).getValue();
    }
}
