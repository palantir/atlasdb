/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

public class CassandraNodeIdentifier {
    // The hostname of Cassandra host
    private final String hostname;
    // The most recently used IP address
    private InetAddress lastUsedIpAddress;
    // The port number of the Socket Address
    private final int port;
    // The list of IP addresses associated with this Cassandra host
    private final Set<InetAddress> knownAssociatedIpAddresses = new HashSet<>();

    private CassandraNodeIdentifier(String hostname, InetAddress inetAddress, int port) {
        this.hostname = hostname != null ? hostname : inetAddress.getHostName();
        this.lastUsedIpAddress = inetAddress;
        this.port = port;
    }

    public static CassandraNodeIdentifier create(String hostname, int port) {
        checkHost(hostname);
        InetAddress addr = null;
        String host = null;
        try {
            addr = InetAddress.getByName(hostname);
        } catch (UnknownHostException e) {
            host = hostname;
        }
        return new CassandraNodeIdentifier(host, addr, checkPort(port));
    }

    public static CassandraNodeIdentifier create(InetAddress address, int port) {
        return new CassandraNodeIdentifier(null, address, checkPort(port));
    }

    public static CassandraNodeIdentifier from(InetSocketAddress inetSocketAddress) {
        return new CassandraNodeIdentifier(null, inetSocketAddress.getAddress(), inetSocketAddress.getPort());
    }
    // todo(snanda): this is also dupe

    public String getHostName() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    // todo(snanda) concurrency?
    public void updateMostRecentIpAddress(InetAddress newAddress) {
        if (lastUsedIpAddress.equals(newAddress)) {
            return;
        }
        knownAssociatedIpAddresses.add(lastUsedIpAddress);
        lastUsedIpAddress = newAddress;
    }

    public void vacateLastUsedIpAddress() {
        updateMostRecentIpAddress(null);
    }

    public InetAddress getLastUsedIpAddress() {
        return lastUsedIpAddress;
    }

    public boolean recognizes(InetAddress resolvedHost) {
        return hostname.equals(resolvedHost.getHostName()) || recognizesInetAddress(resolvedHost);
    }

    private boolean recognizesInetAddress(InetAddress resolvedHost) {
        return lastUsedIpAddress.equals(resolvedHost) || knownAssociatedIpAddresses.contains(resolvedHost);
    }

    // todo(snanda): dupe
    private static String checkHost(String hostname) {
        if (hostname == null) throw new IllegalArgumentException("hostname can't be null");
        return hostname;
    }

    private static int checkPort(int port) {
        if (port < 0 || port > 0xFFFF) throw new IllegalArgumentException("port out of range:" + port);
        return port;
    }

    public void addAddress(InetAddress inetAddress) {
        knownAssociatedIpAddresses.add(inetAddress);
    }

    // todo(snanda): verify correctness
    public String getHostString() {
        if (hostname != null) return hostname;
        if (lastUsedIpAddress != null) {
            return lastUsedIpAddress.getHostAddress();
        }
        return null;
    }
}
