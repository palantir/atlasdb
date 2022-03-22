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
package com.palantir.atlasdb.util;

import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.simple.test.SimpleTestServer;
import java.net.URI;
import org.junit.rules.ExternalResource;

public class ServerRule extends ExternalResource {

    private final SimpleTestServer server;

    public ServerRule(Object... resources) {
        SimpleTestServer.Builder builder = SimpleTestServer.builder().contextPath("/application");
        for (Object object : resources) {
            if (object instanceof Class) {
                builder.jersey(jerseyConfig -> jerseyConfig.register((Class<?>) object));
            } else if (object instanceof UndertowService) {
                builder.undertow((UndertowService) object);
            } else {
                builder.jersey(jerseyConfig -> jerseyConfig.register(object));
            }
        }
        server = builder.build();
    }

    public URI baseUri() {
        return URI.create("http://localhost:" + getLocalPort() + "/application");
    }

    private int getLocalPort() {
        return server.getPort();
    }

    @Override
    protected void before() throws Throwable {
        server.start();
    }

    @Override
    protected void after() {
        server.stop();
    }
}
