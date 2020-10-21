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
package com.palantir.nexus.db.pool.config;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum ConnectionProtocol {
    TCP("tcp"),
    TCPS("tcps");

    private final String urlStr;

    ConnectionProtocol(String urlStr) {
        this.urlStr = urlStr;
    }

    public String getUrlString() {
        return urlStr;
    }

    @JsonCreator
    public static ConnectionProtocol fromUrlString(String val) {
        for (ConnectionProtocol cp : ConnectionProtocol.values()) {
            if (cp.getUrlString().equalsIgnoreCase(val)) {
                return cp;
            }
        }
        throw new IllegalArgumentException(val + " does not correspond to a known ConnectionProtocol");
    }
}
