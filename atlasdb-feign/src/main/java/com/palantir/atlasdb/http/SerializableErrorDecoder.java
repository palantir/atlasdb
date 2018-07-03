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
package com.palantir.atlasdb.http;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HttpHeaders;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.common.remoting.HeaderAccessUtils;
import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.errors.SerializableErrorToExceptionConverter;

import feign.Response;
import feign.codec.ErrorDecoder;

public class SerializableErrorDecoder implements ErrorDecoder {
    private static final Logger log = LoggerFactory.getLogger(SerializableErrorDecoder.class);

    /**
     * Returns an AtlasDbRemoteException if the response may be decoded as such; otherwise returns a
     * generic RuntimeException (for instance, if the response is empty or malformed).
     */
    @Override
    public Exception decode(String methodKey, Response response) {
        return wrapRemoteException(SerializableErrorToExceptionConverter.getException(
                HeaderAccessUtils.shortcircuitingCaseInsensitiveGet(response.headers(), HttpHeaders.CONTENT_TYPE),
                response.status(),
                response.reason(),
                getBody(response)));
    }

    private static InputStream getBody(Response response) {
        try {
            if (response.body() != null) {
                return response.body().asInputStream();
            }
        } catch (IOException e) {
            log.warn("Unable to read message body from response {}", response, e);
        }
        return null;
    }

    private static Exception wrapRemoteException(RuntimeException exception) {
        log.info("EXCEPTIONNNNN: {}", exception);
        if (exception instanceof RemoteException) {
            return new AtlasDbRemoteException((RemoteException) exception);
        }
        return exception;
    }
}
