// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.timestamp;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DatabaseIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final DatabaseIdentifier FAKE = new DatabaseIdentifier("FAKE", -1, "FAKE");

    private final String host;
    private final int port;
    private final String user;

    public DatabaseIdentifier(String host, int port, String user) {
        this.host = host;
        this.port = port;
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public boolean semanticEquals(DatabaseIdentifier other) {
        if (!user.equals(other.user)) {
            return false;
        }
        if (port != other.port) {
            return false;
        }
        if (host.equals(other.host)) {
            return true;
        }
        try {
            Set<InetAddress> ourAddresses = new HashSet<InetAddress>();
            Collections.addAll(ourAddresses, InetAddress.getAllByName(host));
            Set<InetAddress> otherAddresses = new HashSet<InetAddress>();
            Collections.addAll(otherAddresses, InetAddress.getAllByName(other.host));
            if (!Collections.disjoint(ourAddresses, otherAddresses)) {
                return true;
            }
        } catch (UnknownHostException e) {
            // ignore
        }
        return false;
    }

    @Override
    public String toString() {
        return "DatabaseIdentifier [host=" + host + ", port=" + port + ", user=" + user + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        result = prime * result + ((user == null) ? 0 : user.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DatabaseIdentifier other = (DatabaseIdentifier) obj;
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host)) {
            return false;
        }
        if (port != other.port) {
            return false;
        }
        if (user == null) {
            if (other.user != null) {
                return false;
            }
        } else if (!user.equals(other.user)) {
            return false;
        }
        return true;
    }
}
