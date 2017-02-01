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

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;

public class CheckAndSetExceptionMapper implements ExceptionMapper<CheckAndSetException> {
    private static final Logger log = LoggerFactory.getLogger(CheckAndSetExceptionMapper.class);

    @Override
    public Response toResponse(CheckAndSetException ex) {
        LockEntry actualEntry = extractStoredLockEntry(ex);
        return createReleaseErrorResponse(actualEntry);
    }

    private LockEntry extractStoredLockEntry(CheckAndSetException ex) {
        // Want a slightly different response if the lock was already open
        List<byte[]> actualValues = ex.getActualValues();
        if (actualValues == null || actualValues.size() != 1) {
            // Rethrow - something odd happened in the db, and here we _do_ want the log message/stack trace.
            throw ex;
        }

        byte[] rowName = ex.getKey().getRowName();
        byte[] actualValue = Iterables.getOnlyElement(actualValues);
        return LockEntry.fromRowAndValue(rowName, actualValue);
    }

    private Response createReleaseErrorResponse(LockEntry actualEntry) {
        log.info("Request failed. Stored LockEntry: {}", actualEntry);
        String message = LockStore.LOCK_OPEN.equals(actualEntry)
                ? "The lock has already been released"
                : String.format("Another lock has been taken out: %s", actualEntry);
        return Response.status(Response.Status.CONFLICT).entity(Entity.text(message)).build();
    }
}
