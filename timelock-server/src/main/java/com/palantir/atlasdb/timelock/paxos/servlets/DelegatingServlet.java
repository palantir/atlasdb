/**
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.paxos.servlets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.remoting1.errors.SerializableError;
import java.io.IOException;
import java.util.concurrent.Callable;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.http.HttpStatus;

public class DelegatingServlet<V> extends HttpServlet {
    private final Callable<V> function;
    private final MediaType mediaType;
    private final ObjectMapper mapper;
    private final HttpMethod allowedMethod;

    public DelegatingServlet(
            Callable<V> function, MediaType mediaType, ObjectMapper mapper, HttpMethod allowedMethod) {
        this.function = function;
        this.mediaType = mediaType;
        this.mapper = mapper;
        this.allowedMethod = allowedMethod;
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        writeResponseIfMethodMatches(response, HttpMethod.GET);
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        writeResponseIfMethodMatches(response, HttpMethod.POST);
    }

    private void writeResponseIfMethodMatches(HttpServletResponse response, String get) throws IOException {
        if (allowedMethod.value().equals(get)) {
            writeResponse(response);
        } else {
            response.sendError(HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    private void writeResponse(HttpServletResponse response) throws IOException {
        response.addHeader(HttpHeaders.CONTENT_TYPE, mediaType.toString());
        try {
            response.getWriter().write(
                    mapper.writeValueAsString(function.call()));
        } catch (Exception e) {
            response.getWriter().write(
                    mapper.writeValueAsString(SerializableError.of(e.getMessage(), e.getClass())));
        }
    }
}
