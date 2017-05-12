/*
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

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ApplicationJsonResponseStrategy<V> implements ResponseStrategy<V> {
    private final ObjectMapper mapper;

    public ApplicationJsonResponseStrategy(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public String mediaType() {
        return MediaType.APPLICATION_JSON;
    }

    @Override
    public void writeResponse(V valueToWrite, HttpServletResponse response) throws IOException {
        response.getWriter().write(mapper.writeValueAsString(valueToWrite));
    }
}
