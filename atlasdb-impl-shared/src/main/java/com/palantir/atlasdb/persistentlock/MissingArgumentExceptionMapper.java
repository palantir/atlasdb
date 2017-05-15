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

package com.palantir.atlasdb.persistentlock;

import java.util.UUID;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class MissingArgumentExceptionMapper implements ExceptionMapper<MissingArgumentException> {
    private static final Logger log = LoggerFactory.getLogger(CheckAndSetExceptionMapper.class);

    @Override
    public Response toResponse(MissingArgumentException ex) {
        String errorId = UUID.randomUUID().toString();
        log.error("Error handling a request: {}. Reason for acquiring the lock not provided.", errorId, ex);
        return createErrorResponse(errorId);
    }

    // This is needed to allow clients using http-remoting clients to properly receive RemoteExceptions
    private Response createErrorResponse(String errorId) {
        return Response
                .status(Response.Status.BAD_REQUEST)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(ImmutableMap.of(
                        "exceptionClass", MissingArgumentException.class.getName(),
                        "message", String.format("Error %s: Reason for acquiring the lock not provided.", errorId),
                        "code", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
    }
}