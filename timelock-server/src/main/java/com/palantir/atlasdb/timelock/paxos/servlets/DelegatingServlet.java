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

import java.io.IOException;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

import org.eclipse.jetty.http.HttpStatus;

import com.google.common.base.Throwables;

public class DelegatingServlet<V> extends HttpServlet {
    private final Callable<V> function;
    private final HttpMethod allowedMethod;
    private final ResponseStrategy<V> responseStrategy;

    public DelegatingServlet(
            Callable<V> function, HttpMethod allowedMethod, ResponseStrategy<V> responseStrategy) {
        this.function = function;
        this.allowedMethod = allowedMethod;
        this.responseStrategy = responseStrategy;
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
        V valueToWrite;
        try {
            valueToWrite = function.call();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        responseStrategy.writeResponse(valueToWrite, response);
    }
}
