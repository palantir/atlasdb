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

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;

public class InsufficientConsistencyExceptionMapper implements
        ExceptionMapper<InsufficientConsistencyException> {

    private final static InsufficientConsistencyExceptionMapper instance = new InsufficientConsistencyExceptionMapper();
    private InsufficientConsistencyExceptionMapper() { }

    public static InsufficientConsistencyExceptionMapper instance() {
        return instance;
    }

    @Override
    public Response toResponse(InsufficientConsistencyException exception) {
        return Response
        		.status(503)
        		.entity("Insufficient consistency!")
        		.header(feign.Util.RETRY_AFTER, "0")
        		.build();
    }

}
