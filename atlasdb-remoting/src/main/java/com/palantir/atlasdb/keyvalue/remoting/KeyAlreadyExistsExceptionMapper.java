/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.remoting;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;

public final class KeyAlreadyExistsExceptionMapper implements ExceptionMapper<KeyAlreadyExistsException> {
    private static final KeyAlreadyExistsExceptionMapper instance = new KeyAlreadyExistsExceptionMapper();

    private KeyAlreadyExistsExceptionMapper() {
        // singleton
    }

    public static KeyAlreadyExistsExceptionMapper instance() {
        return instance;
    }

    @Override
    public Response toResponse(KeyAlreadyExistsException exception) {
        return Response
                .status(409)
                .entity("Key already exists!")
                .build();
    }
}
