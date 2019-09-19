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
package com.palantir.atlasdb.persistentlock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.logsafe.SafeArg;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckAndSetExceptionMapper implements ExceptionMapper<CheckAndSetException> {
    private static final Logger log = LoggerFactory.getLogger(CheckAndSetExceptionMapper.class);

    @Override
    public Response toResponse(CheckAndSetException ex) {
        String errorId = UUID.randomUUID().toString();
        LockEntry lockEntry = extractStoredLockEntry(ex);
        log.error("Error handling a request: {}. Stored persistent lock: {}",
                SafeArg.of("errorId", errorId),
                SafeArg.of("lockEntry", lockEntry),
                ex);
        return createErrorResponse(errorId, lockEntry);
    }

    private LockEntry extractStoredLockEntry(CheckAndSetException ex) {
        // Want a slightly different response if the lock was already open
        List<byte[]> actualValues = ex.getActualValues();
        if (actualValues == null || actualValues.size() != 1) {
            // something odd happened in the db
            return null;
        }

        byte[] actualValue = Iterables.getOnlyElement(actualValues);
        return LockEntry.fromStoredValue(actualValue);
    }

    // This is needed to allow clients using http-remoting clients to properly receive RemoteExceptions
    private Response createErrorResponse(String errorId, LockEntry lockEntry) {
        String message = String.format("Error %s: Check and set failed. ", errorId)
                + (lockEntry == null ? "Please contact the AtlasDB team. " : lockEntry);
        return Response
                .status(Response.Status.CONFLICT)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(ImmutableMap.of(
                        "exceptionClass", CheckAndSetException.class.getName(),
                        "message", message,
                        "code", Response.Status.CONFLICT.getStatusCode()))
                .build();
    }
}
