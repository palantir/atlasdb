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

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;

public class DockerizedDatabaseUri {

    private static final String DELIMITER = "@";

    private final KeyValueServiceInstrumentation type;
    private final InetSocketAddress addr;

    public DockerizedDatabaseUri(KeyValueServiceInstrumentation type, InetSocketAddress addr) {
        this.type = type;
        this.addr = addr;
    }

    public static DockerizedDatabaseUri fromUriString(String uri) {
        String[] parts = uri.trim().split(DELIMITER);

        DockerizedDatabaseUri d = null;
        try {
            Constructor<KeyValueServiceInstrumentation> test = (Constructor<KeyValueServiceInstrumentation>) Class.forName(
                    parts[0]).getConstructors()[0];
            KeyValueServiceInstrumentation bla = test.newInstance();
            String[] addrParts = parts[2].split(":");

            d = new DockerizedDatabaseUri(
                    bla,
                    InetSocketAddress.createUnresolved(addrParts[0], Integer.parseInt(addrParts[1])));
        }
        catch(Exception e){
            System.exit(1);
        }
        return d;
    }

    public KeyValueServiceInstrumentation getKeyValueServiceInstrumentation() {
        return type;
    }

    public InetSocketAddress getAddress() {
        return addr;
    }

    public String toString() {
        return type.toString() + DELIMITER + addr.toString();
    }

}
