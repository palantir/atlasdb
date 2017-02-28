/**
 * Copyright 2017 Palantir Technologies
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

import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;

public class CheckAndSetExceptionMapper implements ExceptionMapper<CheckAndSetException> {
    private static final Logger log = LoggerFactory.getLogger(CheckAndSetExceptionMapper.class);

    @Override
    public Response toResponse(CheckAndSetException ex) {
        String errorId = UUID.randomUUID().toString();
        log.error("Error handling a request: {}. Stored persistent lock: {}", errorId, extractStoredLockEntry(ex), ex);
        return createErrorResponse(errorId);
    }

    private LockEntry extractStoredLockEntry(CheckAndSetException ex) {
        // Want a slightly different response if the lock was already open
        List<byte[]> actualValues = ex.getActualValues();
        if (actualValues == null || actualValues.size() != 1) {
            // Rethrow - something odd happened in the db, and here we _do_ want the log message/stack trace.
            throw ex;
        }

        byte[] actualValue = Iterables.getOnlyElement(actualValues);
        return LockEntry.fromStoredValue(actualValue);
    }

    // This is needed to allow clients using http-remoting clients to properly receive RemoteExceptions
    private Response createErrorResponse(String errorId) {
        return Response
                .status(Response.Status.CONFLICT)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(ImmutableMap.of(
                        "exceptionClass", CheckAndSetException.class.getName(),
                        "message", String.format("Error %s: Check and set failed.", errorId),
                        "code", Response.Status.CONFLICT.getStatusCode()))
                .build();
    }
}
