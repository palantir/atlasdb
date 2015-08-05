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
package com.palantir.atlasdb.shell;

public class DbConnectionInfo {
    private final String type;
    private final String host;
    private final String port;
    private final String identifier;
    private final String username;
    private final String password;

    public static DbConnectionInfo create(String host,
                             String port,
                             String identifier,
                             String type,
                             String username,
                             String password) {
        return new DbConnectionInfo(host, port, identifier, type, username, password);
    }

    private DbConnectionInfo(String host,
                             String port,
                             String identifier,
                             String type,
                             String username,
                             String password) {
        this.host = host;
        this.port = port;
        this.identifier = identifier;
        this.type = type;
        this.username = username;
        this.password = password;
    }

    public String getType() {
        return type;
    }
    public String getHost() {
        return host;
    }
    public String getPort() {
        return port;
    }
    public String getIdentifier() {
        return identifier;
    }
    public String getUsername() {
        return username;
    }
    public String getPassword() {
        return password;
    }
}
