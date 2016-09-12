/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.performance.backend;

import java.net.InetSocketAddress;

public class DockerizedDatabaseUri {

    private static final String DELIMITER = "@";

    private final KeyValueServiceType type;
    private final InetSocketAddress addr;

    public DockerizedDatabaseUri(KeyValueServiceType type, InetSocketAddress addr) {
        this.type = type;
        this.addr = addr;
    }

    public static DockerizedDatabaseUri fromUriString(String uri) {
        String[] parts = uri.split(DELIMITER);
        String[] addrParts = parts[1].split(":");
        return new DockerizedDatabaseUri(
                KeyValueServiceType.valueOf(parts[0]),
                new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1])));
    }

    public KeyValueServiceType getKeyValueServiceType() {
        return type;
    }

    public InetSocketAddress getAddress() {
        return addr;
    }

    public String toString() {
        return type.toString() + DELIMITER + addr.toString();
    }

}
