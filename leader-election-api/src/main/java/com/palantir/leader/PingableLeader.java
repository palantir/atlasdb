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
package com.palantir.leader;

import com.palantir.atlasdb.metrics.Timed;
import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.conjure.java.undertow.annotations.SerializerFactory;
import com.palantir.conjure.java.undertow.lib.Endpoint;
import com.palantir.conjure.java.undertow.lib.Serializer;
import com.palantir.conjure.java.undertow.lib.TypeMarker;
import com.palantir.conjure.java.undertow.lib.UndertowRuntime;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/leader")
public interface PingableLeader {

    /**
     * If this call returns then the server is reachable.
     *
     * @return true if the remote server thinks it is the leader, otherwise false
     */
    @GET
    @Path("ping")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    @Handle(method = HttpMethod.GET, path = "/leader/ping")
    boolean ping();

    /**
     * Returns a unique string identifier for the leader election service.
     */
    @GET
    @Path("uuid")
    @Produces(MediaType.TEXT_PLAIN)
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Avoiding API break
    @Timed
    @Handle(method = HttpMethod.GET, path = "/leader/uuid", produces = TextPlainSerializer.class)
    String getUUID();

    /**
     * If this call returns then the server is reachable.
     *
     * @return boolean to represent if the server thinks it is the leader or not along with string
     * version of TimeLock on remote server
     */
    @GET
    @Path("pingV2")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    @Handle(method = HttpMethod.GET, path = "/leader/pingV2")
    PingResult pingV2();

    enum TextPlainSerializer implements SerializerFactory<String> {
        INSTANCE;

        @Override
        public <T extends String> Serializer<T> serializer(
                TypeMarker<T> type, UndertowRuntime runtime, Endpoint endpoint) {
            return this::serialize;
        }

        private void serialize(String value, HttpServerExchange exchange) throws IOException {
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, MediaType.TEXT_PLAIN);
            exchange.getOutputStream().write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
